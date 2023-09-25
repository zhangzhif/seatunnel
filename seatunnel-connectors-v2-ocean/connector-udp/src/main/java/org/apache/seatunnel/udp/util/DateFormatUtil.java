package org.apache.seatunnel.udp.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @author 徐正洲
 * @create 2022-10-31 16:56
 *     <p>时间工具类
 */
public class DateFormatUtil {
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter dtfFull =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    public static Long toTs(String dtStr, boolean isFull) {

        LocalDateTime localDateTime = null;
        if (!isFull) {
            dtStr = dtStr + " 00:00:00";
        }
        localDateTime = LocalDateTime.parse(dtStr, dtfFull);

        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

    public static Long toTs(String dtStr) {
        return toTs(dtStr, false);
    }

    public static String toDate(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime =
                LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dtf.format(localDateTime);
    }

    public static String toYmdHms(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime =
                LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dtfFull.format(localDateTime);
    }

    public static Long getTs(String ts) throws ParseException {
        // 距离今天0点的时间戳
        //        Long currentDayTs = Long.parseLong(ts);
        //        Long currentDayMs =currentDayTs /10L;
        //
        //        //今天日期的时间戳
        //        LocalDate now = LocalDate.now();
        //        Date date = sdf.parse(now.toString());
        //        long time = date.getTime();
        //
        //        Long rsTs = currentDayMs + time;
        return System.currentTimeMillis();
    }

    public static void main(String[] args) throws ParseException {
        System.out.println(DateFormatUtil.toYmdHms(1676612548184l));
    }
}
