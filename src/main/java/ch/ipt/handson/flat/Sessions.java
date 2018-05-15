package ch.ipt.handson.flat;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Sessions {

    public static final int CONCURRENT_SESSIONS = 30;
    public static final double CHURN_RATE_PER_SECOND = 1.0;

    static final ConcurrentMap<String, String> openSessions = new ConcurrentHashMap<>();
    static private Random random = new Random();

    public class Session {
//        public
    }

    public Sessions() {


//
//
//        UUID.randomUUID().toString(;


    }

    public static ConcurrentMap<String, String> getOpenSessions() {
        return openSessions;
    }
}
