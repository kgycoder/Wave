package com.sync.app;

import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URLEncoder;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * "실제로 등록된 음악" 검색.
 *  - 카탈로그(iTunes · Deezer: 싱글/EP/앨범에 등록된 곡 목록)와 YouTube Music 을 병렬 조회해 병합한다.
 *  - YouTube Music 곡 검색에 없는 카탈로그 곡(앨범 안에만 있는 곡)은 YTM 에서 같은 곡을 찾아 재생 가능한
 *    videoId 로 연결하고, 그래도 없으면 YTM 앨범 페이지를 열어 트랙 목록에서 찾는다.
 *  - 외부 API 실패/지연은 조용히 무시 → 항상 YTM 단독 결과로 폴백(검색 속도 유지).
 */
final class MusicCatalog {

    interface Host {
        JSONArray ytm(String query, String kind) throws Exception;   // YTM 검색 (song/album/artist)
        JSONObject browse(String browseId) throws Exception;         // YTM 앨범/아티스트 페이지
        String get(String url, int timeoutMs) throws Exception;      // 외부 GET (UTF-8)
    }

    private static final long CAT_TTL = 5 * 60_000L;
    private static final int CAT_WAIT_MS = 2500, RESOLVE_WAIT_MS = 2400;

    private final Host h;
    private final ExecutorService pool;

    MusicCatalog(Host host, ExecutorService pool) { this.h = host; this.pool = pool; }

    /* ───────── 데이터 ───────── */
    private static final class CT {            // 카탈로그 곡
        String title = "", artist = "", album = "", year = "", art = "";
        int dur, rank, src;                     // src: 1=iTunes 2=Deezer
        double rel;
    }
    private static final class CA {            // 카탈로그 앨범
        String name = "", artist = "", year = "";
        int rank, src;
        double rel;
    }
    private static final class Cat {
        final List<CT> tracks = Collections.synchronizedList(new ArrayList<>());
        final List<CA> albums = Collections.synchronizedList(new ArrayList<>());
    }

    private final LinkedHashMap<String, Object[]> cache = new LinkedHashMap<String, Object[]>(16, .75f, true) {
        @Override protected boolean removeEldestEntry(Map.Entry<String, Object[]> e) { return size() > 24; }
    };

    private Future<Cat> catalog(final String q) {
        final String key = q.toLowerCase();
        synchronized (cache) {
            Object[] c = cache.get(key);
            if (c != null && System.currentTimeMillis() - (Long) c[0] < CAT_TTL) {
                @SuppressWarnings("unchecked") Future<Cat> f = (Future<Cat>) c[1];
                return f;
            }
            final CompletableFuture<Cat> cf = new CompletableFuture<>();
            cache.put(key, new Object[]{System.currentTimeMillis(), cf});
            pool.submit(() -> {
                final Cat cat = new Cat();
                final long t0 = System.currentTimeMillis();
                final String enc = urlEnc(q);
                List<Future<?>> fs = new ArrayList<>();
                fs.add(pool.submit(() -> itunes(cat, enc, true)));
                fs.add(pool.submit(() -> itunes(cat, enc, false)));
                fs.add(pool.submit(() -> deezer(cat, enc, true)));
                fs.add(pool.submit(() -> deezer(cat, enc, false)));
                for (Future<?> f : fs) {
                    long left = CAT_WAIT_MS - (System.currentTimeMillis() - t0);
                    try { f.get(Math.max(left, 1), TimeUnit.MILLISECONDS); } catch (Exception ignored) { }
                }
                cf.complete(cat);
            });
            return cf;
        }
    }

    private static String urlEnc(String s) {
        try { return URLEncoder.encode(s, "UTF-8"); } catch (Exception e) { return ""; }
    }

    private void itunes(Cat cat, String enc, boolean songs) {
        try {
            String js = h.get("https://itunes.apple.com/search?media=music&country=KR&limit=" + (songs ? 25 : 12)
                    + "&entity=" + (songs ? "song" : "album") + "&term=" + enc, 3000);
            JSONArray a = new JSONObject(js).optJSONArray("results");
            if (a == null) return;
            for (int i = 0; i < a.length(); i++) {
                JSONObject o = a.optJSONObject(i);
                if (o == null) continue;
                String yr = o.optString("releaseDate", "");
                yr = yr.length() >= 4 ? yr.substring(0, 4) : "";
                if (songs) {
                    if (!"song".equals(o.optString("wrapperType", "track")) && !"track".equals(o.optString("wrapperType", "track"))) continue;
                    CT t = new CT();
                    t.title = o.optString("trackName"); t.artist = o.optString("artistName");
                    t.album = o.optString("collectionName"); t.year = yr;
                    t.dur = o.optInt("trackTimeMillis") / 1000; t.rank = i; t.src = 1;
                    if (!t.title.isEmpty()) cat.tracks.add(t);
                } else {
                    CA al = new CA();
                    al.name = o.optString("collectionName"); al.artist = o.optString("artistName");
                    al.year = yr; al.rank = i; al.src = 1;
                    if (!al.name.isEmpty()) cat.albums.add(al);
                }
            }
        } catch (Exception ignored) { }
    }

    private void deezer(Cat cat, String enc, boolean songs) {
        try {
            String js = h.get("https://api.deezer.com/search/" + (songs ? "track" : "album")
                    + "?limit=" + (songs ? 25 : 12) + "&q=" + enc, 3000);
            JSONArray a = new JSONObject(js).optJSONArray("data");
            if (a == null) return;
            for (int i = 0; i < a.length(); i++) {
                JSONObject o = a.optJSONObject(i);
                if (o == null) continue;
                JSONObject ar = o.optJSONObject("artist");
                String artist = ar == null ? "" : ar.optString("name");
                if (songs) {
                    JSONObject al = o.optJSONObject("album");
                    CT t = new CT();
                    t.title = o.optString("title"); t.artist = artist;
                    t.album = al == null ? "" : al.optString("title");
                    t.dur = o.optInt("duration"); t.rank = i; t.src = 2;
                    if (!t.title.isEmpty()) cat.tracks.add(t);
                } else {
                    CA al = new CA();
                    al.name = o.optString("title"); al.artist = artist; al.rank = i; al.src = 2;
                    if (!al.name.isEmpty()) cat.albums.add(al);
                }
            }
        } catch (Exception ignored) { }
    }

    /* ───────── 문자열 유틸 ───────── */
    private static String norm(String s) {
        if (s == null) return "";
        String d = Normalizer.normalize(s, Normalizer.Form.NFD).replaceAll("\\p{M}+", "").toLowerCase();
        return d.replaceAll("[^\\p{L}\\p{N}]+", " ").trim();
    }
    /** (feat. …) 류와 "- Single"/"- EP" 꼬리표 제거 — 곡/앨범 동일성 비교용 */
    private static String core(String s) {
        if (s == null) return "";
        String x = s.replaceAll("(?i)[\\(\\[]\\s*(feat|ft|with|prod)[^\\)\\]]*[\\)\\]]", " ")
                .replaceAll("(?i)\\s[-–]\\s*(feat|ft)\\.?\\s.*$", " ")
                .replaceAll("(?i)\\s[-–]\\s*(single|ep)\\s*$", " ");
        return norm(x);
    }
    private static Set<String> toks(String n) {
        Set<String> t = new HashSet<>();
        if (!n.isEmpty()) for (String w : n.split(" ")) if (!w.isEmpty()) t.add(w);
        return t;
    }
    private static double sim(String a, String b) {
        if (a.isEmpty() || b.isEmpty()) return 0;
        if (a.equals(b)) return 1;
        Set<String> x = toks(a), y = toks(b);
        int inter = 0;
        for (String w : x) if (y.contains(w)) inter++;
        double j = inter / (double) (x.size() + y.size() - inter);
        if ((a.contains(b) || b.contains(a)) && Math.min(a.length(), b.length()) >= .7 * Math.max(a.length(), b.length()))
            j = Math.max(j, .9);
        return j;
    }
    private static boolean artistMatch(String a, String b) {
        String x = norm(a), y = norm(b);
        if (x.isEmpty() || y.isEmpty()) return true;
        if (x.contains(y) || y.contains(x)) return true;
        return sim(x, y) >= .5;
    }
    private static boolean durOk(int a, int b, int tol) { return a <= 0 || b <= 0 || Math.abs(a - b) <= tol; }

    /** 질의 토큰이 문자열에 얼마나 포함되는지 (0~1) */
    private static double coverage(String nq, String hay) {
        if (nq.isEmpty()) return 0;
        String[] q = nq.split(" ");
        int hit = 0;
        for (String w : q) if (hay.contains(w)) hit++;
        return hit / (double) q.length;
    }

    /* ───────── 곡 검색 ───────── */
    JSONArray searchSongs(String q) throws Exception {
        final long t0 = System.currentTimeMillis();
        Future<Cat> fc = catalog(q);
        Future<JSONArray> fy = pool.submit((Callable<JSONArray>) () -> h.ytm(q, "song"));
        JSONArray yt;
        try { yt = fy.get(12, TimeUnit.SECONDS); } catch (Exception e) { yt = new JSONArray(); }
        Cat cat = new Cat();
        try { cat = fc.get(Math.max(300, CAT_WAIT_MS + 400 - (System.currentTimeMillis() - t0)), TimeUnit.MILLISECONDS); }
        catch (Exception ignored) { }

        final String nq = norm(q);
        final List<JSONObject> items = new ArrayList<>();
        final List<double[]> meta = new ArrayList<>();      // {score, ytmRank}
        final Map<String, Integer> byId = new HashMap<>();
        for (int i = 0; i < yt.length(); i++) {
            JSONObject o = yt.optJSONObject(i);
            if (o == null || byId.containsKey(o.optString("id"))) continue;
            byId.put(o.optString("id"), items.size());
            items.add(o);
            meta.add(new double[]{3.0 / (1 + .35 * i) + 1.5, i});
        }

        /* 카탈로그 곡 중복 제거(iTunes+Deezer 동일 곡은 합쳐 합의 점수↑) */
        List<CT> cts = new ArrayList<>();
        Map<String, CT> seen = new HashMap<>();
        List<CT> snap;
        synchronized (cat.tracks) { snap = new ArrayList<>(cat.tracks); }
        for (CT t : snap) {
            t.rel = coverage(nq, norm(t.title + " " + t.artist + " " + t.album));
            String k = core(t.title) + "|" + core(t.artist).split(" ")[0];
            CT p = seen.get(k);
            if (p != null && durOk(p.dur, t.dur, 4)) { p.src |= t.src; p.rank = Math.min(p.rank, t.rank); if (p.year.isEmpty()) p.year = t.year; continue; }
            seen.put(k, t); cts.add(t);
        }

        /* YTM 결과와 대조: 이미 있으면 점수/메타 보강, 없으면 해결 대상 */
        List<CT> missing = new ArrayList<>();
        for (CT t : cts) {
            int hit = -1;
            String ct = core(t.title);
            for (int i = 0; i < items.size() && hit < 0; i++) {
                JSONObject o = items.get(i);
                if (sim(ct, core(o.optString("title"))) >= .85 && artistMatch(t.artist, o.optString("channel"))
                        && durOk(t.dur, o.optInt("dur"), 5)) hit = i;
            }
            if (hit >= 0) {
                meta.get(hit)[0] += (t.src == 3 ? 2.4 : 1.2) + 3.0 / (1 + .35 * t.rank);
                JSONObject o = items.get(hit);
                try {
                    if (o.optString("year").isEmpty() && !t.year.isEmpty()) o.put("year", t.year);
                    if (o.optString("album").isEmpty() && !t.album.isEmpty()) o.put("album", t.album);
                } catch (Exception ignored) { }
            } else if (t.rel >= .6) missing.add(t);
        }
        Collections.sort(missing, (a, b) -> Double.compare(b.rel * 10 - b.rank * .2 + (b.src == 3 ? 1 : 0),
                a.rel * 10 - a.rank * .2 + (a.src == 3 ? 1 : 0)));
        if (missing.size() > 5) missing = new ArrayList<>(missing.subList(0, 5));

        /* 앨범 안에만 있는 곡 → YTM 에서 재생 가능한 항목으로 연결 */
        if (!missing.isEmpty()) {
            List<Future<JSONObject>> fs = new ArrayList<>();
            for (final CT t : missing) fs.add(pool.submit(() -> resolveTrack(t)));
            long t1 = System.currentTimeMillis();
            for (int i = 0; i < fs.size(); i++) {
                JSONObject r;
                try { r = fs.get(i).get(Math.max(1, RESOLVE_WAIT_MS - (System.currentTimeMillis() - t1)), TimeUnit.MILLISECONDS); }
                catch (Exception e) { r = null; }
                if (r == null) continue;
                CT t = missing.get(i);
                Integer at = byId.get(r.optString("id"));
                double add = 1.5 + (t.src == 3 ? 2.4 : 1.2) + 3.0 / (1 + .35 * t.rank);
                if (at != null) { meta.get(at)[0] += add; continue; }
                byId.put(r.optString("id"), items.size());
                items.add(r);
                meta.add(new double[]{add, 99});
            }
        }

        /* 최종 점수: 질의 일치도 + 출처 합의 + 순위 */
        Integer[] ord = new Integer[items.size()];
        double[] sc = new double[items.size()];
        for (int i = 0; i < ord.length; i++) {
            ord[i] = i;
            JSONObject o = items.get(i);
            String nt = core(o.optString("title"));
            double cov = coverage(nq, norm(o.optString("title") + " " + o.optString("channel") + " " + o.optString("album")));
            sc[i] = meta.get(i)[0] + cov * 10 + (nt.equals(core(q)) ? 5 : (!nt.isEmpty() && nt.startsWith(nq) ? 2 : 0));
        }
        java.util.Arrays.sort(ord, (a, b) -> Double.compare(sc[b], sc[a]));

        JSONArray out = new JSONArray();
        HashSet<String> dedupe = new HashSet<>();
        for (int k : ord) {
            JSONObject o = items.get(k);
            String key = core(o.optString("title")) + "|" + core(o.optString("channel")) + "|" + core(o.optString("album"));
            if (!dedupe.add(key)) continue;
            out.put(o);
            if (out.length() >= 24) break;
        }
        return out;
    }

    /** 카탈로그 곡 1개를 YTM 의 재생 가능한 곡 항목으로 변환 (실패 시 null) */
    private JSONObject resolveTrack(CT t) {
        String ct = core(t.title);
        try {   // 1) YTM 곡 검색에서 제목·아티스트·길이로 검증
            JSONArray a = h.ytm(t.title + " " + t.artist, "song");
            JSONObject best = null; double bs = 0;
            for (int i = 0; i < a.length(); i++) {
                JSONObject o = a.optJSONObject(i);
                if (o == null) continue;
                double s = sim(ct, core(o.optString("title")));
                boolean am = artistMatch(t.artist, o.optString("channel"));
                boolean dm = o.optInt("dur") > 0 && t.dur > 0 && Math.abs(o.optInt("dur") - t.dur) <= 2;
                if (s >= .85 && (am || dm) && durOk(t.dur, o.optInt("dur"), 5)) {
                    double v = s + (am ? .3 : 0) + (dm ? .2 : 0) - i * .01;
                    if (v > bs) { bs = v; best = o; }
                }
            }
            if (best != null) {
                if (best.optString("year").isEmpty() && !t.year.isEmpty()) best.put("year", t.year);
                return best;
            }
        } catch (Exception ignored) { }
        if (t.album.isEmpty()) return null;
        try {   // 2) 앨범 페이지의 트랙 목록에서 찾기 (검색 결과에 노출되지 않는 앨범 전용 곡)
            JSONObject al = findAlbum(t.album, t.artist);
            if (al == null) return null;
            JSONObject page = h.browse(al.optString("id"));
            JSONArray secs = page.optJSONArray("sections");
            if (secs == null) return null;
            for (int s = 0; s < secs.length(); s++) {
                JSONArray its = secs.getJSONObject(s).optJSONArray("items");
                if (its == null) continue;
                for (int i = 0; i < its.length(); i++) {
                    JSONObject o = its.optJSONObject(i);
                    if (o == null || !"song".equals(o.optString("kind"))) continue;
                    if (sim(ct, core(o.optString("title"))) < .85 || !durOk(t.dur, o.optInt("dur"), 6)) continue;
                    if (o.optString("channel").isEmpty()) o.put("channel", t.artist.isEmpty() ? page.optString("artist") : t.artist);
                    if (o.optString("artistId").isEmpty()) o.put("artistId", page.optString("artistId"));
                    o.put("album", page.optString("name", t.album));
                    o.put("albumId", al.optString("id"));
                    if (o.optString("year").isEmpty()) o.put("year", page.optString("year", t.year));
                    String th = page.optString("thumb");
                    if (!th.isEmpty()) o.put("art", th);
                    return o;
                }
            }
        } catch (Exception ignored) { }
        return null;
    }

    private JSONObject findAlbum(String name, String artist) throws Exception {
        String cn = core(name);
        JSONArray a = h.ytm(name + " " + artist, "album");
        JSONObject best = null; double bs = 0;
        for (int i = 0; i < a.length(); i++) {
            JSONObject o = a.optJSONObject(i);
            if (o == null) continue;
            double s = sim(cn, core(o.optString("name")));
            if (s < .85 || !artistMatch(artist, o.optString("artist"))) continue;
            double v = s - i * .01;
            if (v > bs) { bs = v; best = o; }
        }
        return best;
    }

    /* ───────── 앨범 검색 ───────── */
    JSONArray searchAlbums(String q) throws Exception {
        final long t0 = System.currentTimeMillis();
        Future<Cat> fc = catalog(q);
        Future<JSONArray> fy = pool.submit((Callable<JSONArray>) () -> h.ytm(q, "album"));
        JSONArray yt;
        try { yt = fy.get(12, TimeUnit.SECONDS); } catch (Exception e) { yt = new JSONArray(); }
        Cat cat = new Cat();
        try { cat = fc.get(Math.max(300, CAT_WAIT_MS + 400 - (System.currentTimeMillis() - t0)), TimeUnit.MILLISECONDS); }
        catch (Exception ignored) { }

        final String nq = norm(q);
        List<JSONObject> items = new ArrayList<>();
        HashSet<String> ids = new HashSet<>();
        for (int i = 0; i < yt.length(); i++) {
            JSONObject o = yt.optJSONObject(i);
            if (o != null && ids.add(o.optString("id"))) items.add(o);
        }
        List<CA> snap;
        synchronized (cat.albums) { snap = new ArrayList<>(cat.albums); }
        List<CA> missing = new ArrayList<>();
        HashSet<String> seenK = new HashSet<>();
        for (CA c : snap) {
            c.rel = coverage(nq, norm(c.name + " " + c.artist));
            String k = core(c.name) + "|" + core(c.artist);
            if (!seenK.add(k) || c.rel < .6) continue;
            boolean have = false;
            for (JSONObject o : items)
                if (sim(core(c.name), core(o.optString("name"))) >= .85 && artistMatch(c.artist, o.optString("artist"))) { have = true; break; }
            if (!have) missing.add(c);
        }
        Collections.sort(missing, (a, b) -> Double.compare(b.rel * 10 - b.rank * .2, a.rel * 10 - a.rank * .2));
        if (missing.size() > 3) missing = new ArrayList<>(missing.subList(0, 3));
        List<Future<JSONObject>> fs = new ArrayList<>();
        for (final CA c : missing) fs.add(pool.submit(() -> findAlbum(c.name, c.artist)));
        long t1 = System.currentTimeMillis();
        for (Future<JSONObject> f : fs) {
            try {
                JSONObject r = f.get(Math.max(1, RESOLVE_WAIT_MS - (System.currentTimeMillis() - t1)), TimeUnit.MILLISECONDS);
                if (r != null && ids.add(r.optString("id"))) items.add(r);
            } catch (Exception ignored) { }
        }
        JSONArray out = new JSONArray();
        for (JSONObject o : items) { out.put(o); if (out.length() >= 20) break; }
        return out;
    }
}
