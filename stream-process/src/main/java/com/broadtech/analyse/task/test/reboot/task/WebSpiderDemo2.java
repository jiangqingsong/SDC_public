package com.broadtech.analyse.task.test.reboot.task;

import com.broadtech.analyse.task.test.reboot.util.FileTools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author leo.J
 * @description
 * @date 2020-09-09 16:15
 */
public class WebSpiderDemo2 {
    public static void main(String[] args) throws Exception {

        String source = "https://www.qiushibaike.com/text/page/";
        String destUrl = "https://www.qiushibaike.com";

        Set<String> setAll = new HashSet<>();
        for (int index = 1; index <= 13; index++) {
            String tmp = source + index + "/";
            Set<String> urlSet = getArticleUrl(tmp);
            setAll.addAll(urlSet);
        }

        String destPath = "D:/joke.txt";
        File destFile = new File(destPath);
        BufferedWriter bw = new BufferedWriter(new FileWriter(destFile, true));

        Iterator<String> it = setAll.iterator();
        while (it.hasNext()) {
            String articleUrl = destUrl + it.next();
            saveToLocal(articleUrl, bw);
        }
        FileTools.close(bw);
    }

    private static void saveToLocal(String articleUrl, BufferedWriter bw) throws Exception {

        URL url = new URL(articleUrl);

        InputStream is = url.openStream();
        BufferedReader br = new BufferedReader(new InputStreamReader(is));

        String line = null;

        String textRegex = "<span class=\"big-begin\">" + "(.+)";
        Pattern pat = Pattern.compile(textRegex);

        for (int i = 0; i < 500; i++) {
            line = br.readLine();

            if (null != line && line.startsWith("<span class=\"big-begin\">")) {
                Matcher matcher = pat.matcher(line);
                if (matcher.find()) {
                    String text = matcher.group(1).replace("</span>", "").replace("<br/>", "  ");
                    System.out.println(text);
                    bw.write(text);
                    bw.newLine();
                    bw.newLine();
                    i = 500;
                }
            }
        }

        bw.flush();
        FileTools.close(br);
    }

    public static Set<String> getArticleUrl(String source) throws Exception {

        URL url = new URL(source);

        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("user-agent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36");
        BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));

        String line = null;

        String urlRegex = "/article/\\d+";
        Pattern pattern = Pattern.compile(urlRegex);

        Set<String> set = new HashSet<>();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 0; i < 3000; i++) {
            line = br.readLine();

            if (null != line) {

                if (line.contains("target=\"_blank\"")) {
                    Matcher matcher = pattern.matcher(line);
                    if (matcher.find()) {
                        String tar = matcher.group(0);
                        if (map.containsKey(tar)) {
                            set.add(tar);
                        } else {
                            map.put(tar, 1);
                        }
                    }
                }
            }

        }

        if (null != br) {
            br.close();
        }

        return set;
    }

}
