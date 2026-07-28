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

        if (tokens.length < 17) {
            return null;
        }

        try {
            double puLat = Double.parseDouble(tokens[7]);
            double puLong = Double.parseDouble(tokens[6]);
            double doLat = Double.parseDouble(tokens[9]);
            double doLong = Double.parseDouble(tokens[8]);

            if (!AreaMapper.isValidCoord(puLat, puLong) || !AreaMapper.isValidCoord(doLat, doLong)) {
                return null;
            }

            int pickupCellWE = AreaMapper.getWE(puLong);
            int pickupCellNS = AreaMapper.getNS(puLat);
            int dropOffCellWE = AreaMapper.getWE(doLong);
            int dropOffCellNS = AreaMapper.getNS(doLat);

            if (pickupCellWE > AreaMapper.MAX_CELL || pickupCellWE < AreaMapper.MIN_CELL ||
                pickupCellNS > AreaMapper.MAX_CELL || pickupCellNS < AreaMapper.MIN_CELL ||
                dropOffCellWE > AreaMapper.MAX_CELL || dropOffCellWE < AreaMapper.MIN_CELL ||
                dropOffCellNS > AreaMapper.MAX_CELL || dropOffCellNS < AreaMapper.MIN_CELL) {
                return null;
            }

            NYTEventCondensed event = new NYTEventCondensed();
            event.medallion = tokens[0];
            
            synchronized (dateFmt) {
                event.pickupDatetime = dateFmt.parse(tokens[2]);
                event.dropOffDatetime = dateFmt.parse(tokens[3]);
            }
            
            event.pickupCellWE = pickupCellWE;
            event.pickupCellNS = pickupCellNS;
            event.dropOffCellWE = dropOffCellWE;
            event.dropOffCellNS = dropOffCellNS;
            event.fareAmount = Double.parseDouble(tokens[11]);
            event.tipAmount = Double.parseDouble(tokens[14]);
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
