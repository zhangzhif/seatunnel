package org.apache.seatunnel.udp.util;

import cn.hutool.core.util.IdUtil;
import com.alibaba.fastjson.JSONObject;

import java.util.TimeZone;

/**
 * @author 徐正洲
 * @date 2023/11/21 10:04 雷达工具类
 */
public class RadarFormatUtil {

    /**
     * 格式化雷达报文结构
     *
     * @param radarId 雷达来源id
     * @param originData 雷达报文字符串
     * @param height 雷达高度
     * @param position 雷达经纬度
     * @return 雷达json结构化数据
     */
    public static JSONObject formatRadarData(
            String radarId, String originData, double height, double[] position) {
        JSONObject jsonObj = new JSONObject();
        // 主键
        jsonObj.put("record_id", IdUtil.objectId());
        // 批次号
        jsonObj.put("batch_no", ByteConvertUtil.parse(originData.substring(124, 128)));
        // 目标类型
        jsonObj.put("type", "4");
        // 目标距离
        jsonObj.put("distance", ByteConvertUtil.parse(originData.substring(40, 48)));
        // 目标X轴距离(东)
        jsonObj.put("distance_x", ByteConvertUtil.parse(originData.substring(56, 64)));
        // 目标Y轴距离(北)
        jsonObj.put("distance_y", ByteConvertUtil.parse(originData.substring(64, 72)));
        // 目标Z轴距离(上)
        jsonObj.put("distance_z", ByteConvertUtil.parse(originData.substring(72, 80)));
        // 目标方位
        jsonObj.put("bearing", ByteConvertUtil.parse(originData.substring(48, 52)) / 100.00);
        // 目标仰角
        jsonObj.put("pitch", ByteConvertUtil.parse(originData.substring(52, 56)) / 100.00);
        // 目标航向
        jsonObj.put("direction", ByteConvertUtil.parse(originData.substring(96, 100)) / 100.00);
        // 目标航速
        jsonObj.put("speed", ByteConvertUtil.parse(originData.substring(100, 104)) / 10.00);
        // 目标X轴速度(东)
        jsonObj.put("speed_x", ByteConvertUtil.parse(originData.substring(112, 116)) / 10.00);
        // 目标Y轴速度(北)
        jsonObj.put("speed_y", ByteConvertUtil.parse(originData.substring(116, 120)) / 10.00);
        // 目标Z轴速度(上)
        jsonObj.put("speed_z", ByteConvertUtil.parse(originData.substring(120, 124)) / 10.00);
        // 跟踪状态报文
        jsonObj.put("status", ByteConvertUtil.parse(originData.substring(128, 132)));
        // 当前时间
        long ts = getRadarTime(ByteConvertUtil.parse(originData.substring(16, 24)));
        jsonObj.put("create_time", DateFormatUtil.toYmdHms(ts));
        // 时间戳-毫秒
        jsonObj.put("ts", ts);
        // 所属雷达
        jsonObj.put("radar_id", radarId);
        // 根据目标x,y轴转换为经纬度。
        double[] jwdObj =
                JWDUtil.getJwd(
                        position, jsonObj.getDouble("distance_x"), jsonObj.getDouble("distance_y"));
        jsonObj.put("longitude", jwdObj[0]);
        jsonObj.put("latitude", jwdObj[1]);
        // 高度
        double realHeight =
                (jsonObj.getIntValue("distance")
                                * Math.sin(jsonObj.getDouble("pitch") / 180 * Math.PI))
                        + height;
        if (realHeight < 0) {
            jsonObj.put("height", 0);
        } else {
            jsonObj.put("height", realHeight);
        }
        return jsonObj;
    }

    /**
     * 格式化识别报文结构
     *
     * @param data 原始识别报文字符串
     * @param delimiter 字段分隔符
     * @param radarId 雷达id
     * @return 格式化识别json
     */
    public static JSONObject formatTypeData(String data, String delimiter, String radarId) {
        JSONObject targetJson = new JSONObject();
        String[] fields = data.split(delimiter);
        targetJson.put("msg_head", fields[0]);
        targetJson.put("batch_no", fields[1]);
        targetJson.put("type", fields[2]);
        targetJson.put("confidence", fields[3]);
        targetJson.put("create_time", fields[4]);
        targetJson.put("radar_id", radarId);
        return targetJson;
    }

    /**
     * 格式化雷达时间戳
     *
     * @return
     */
    public static long getRadarTime(long radarTs) {
        // 目前时间戳
        long currentTs = System.currentTimeMillis();
        // 当天0点的时间戳
        long nowDayZeroTs =
                currentTs / (1000 * 3600 * 24) * (1000 * 3600 * 24)
                        - TimeZone.getDefault().getRawOffset();
        // 返回时间戳
        return nowDayZeroTs + radarTs / 10;
    }
}
