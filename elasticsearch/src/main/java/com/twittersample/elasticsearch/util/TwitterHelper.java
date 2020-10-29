package com.twittersample.elasticsearch.util;

import com.google.gson.JsonParser;

public class TwitterHelper {

    /**
     *
     * @param tweetJson json string that comes from kafka record.
     * @return tweet id.
     */
    public static String extractIdFromTweet(String tweetJson) {
        return JsonParser.parseString(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

}
