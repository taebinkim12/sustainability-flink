package com.flink.sustainability.NYT;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class NYTEventCondensed {
    private static DateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public String medallion;
    public Date pickupDatetime;
    public Date dropOffDatetime;
    public int pickupCellWE;
    public int pickupCellNS;
    public int dropOffCellWE;
    public int dropOffCellNS;
    public double fareAmount;
    public double tipAmount;

    public static NYTEventCondensed parseLine(String line) {
        String[] tokens = line.split(",");

        if (tokens.length < 9) {
            return null;
        }

        try {
            NYTEventCondensed event = new NYTEventCondensed();
            event.medallion = tokens[0];
            event.pickupDatetime = dateFmt.parse(tokens[1]);
            event.dropOffDatetime = dateFmt.parse(tokens[2]);
            event.pickupCellWE = Integer.parseInt(tokens[3]);
            event.pickupCellNS = Integer.parseInt(tokens[4]);
            event.dropOffCellWE = Integer.parseInt(tokens[5]);
            event.dropOffCellNS = Integer.parseInt(tokens[6]);
            event.fareAmount = Double.parseDouble(tokens[7]);
            event.tipAmount = Double.parseDouble(tokens[8]);
            return event;
        } catch (ParseException | NumberFormatException e) {
            return null;
        }
    }

    public int getPickupCell() {
        return pickupCellNS * (AreaMapper.MAX_CELL - 1) + pickupCellWE;
    }

    public int getDropoffCell() {
        return dropOffCellNS * (AreaMapper.MAX_CELL - 1) + dropOffCellWE;
    }
}
