package com.twittersample.util;

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

    /**
     *
     * @param tweetJson Json string from kafka consumer
     * @return Integer follower count of a tweets owner.
     */
    public static Integer extractUsersFollowersInTweet(String tweetJson) {
        try{
            return JsonParser.parseString(tweetJson)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        }catch (NullPointerException e) {
            return 0;
        }
    }

}
