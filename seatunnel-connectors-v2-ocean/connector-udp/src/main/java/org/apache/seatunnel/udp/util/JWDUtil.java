package org.apache.seatunnel.udp.util;

import org.maptalks.geojson.CRS;
import org.maptalks.geojson.measure.Measurer;

public class JWDUtil {
    // XY坐标系转换成经纬度
    public static double[] getJwd(double[] position, Double distanceX, Double distanceY) {
        // 参数说明: 雷达的经纬度，目标的x距离，目标的y距离，坐标系
        return Measurer.locate(position, distanceX, distanceY, CRS.WGS84);
    }
}
