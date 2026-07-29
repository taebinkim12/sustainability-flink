package com.flink.sustainability.NYT.types;

import com.flink.sustainability.NYT.function.*;

public class NYTDQEvent {
    private static final int NO_FIELDS = 18;
    
    public String vendorId;
    public int pickupCell;
    public int dropoffCell;
    public double tripDistance;

    public static NYTDQEvent parseLine(String line) {
        String[] tokens = line.split(",");

        if (tokens.length != NO_FIELDS) {
            return null;
        }

        try {
            String vendorId = tokens[2];
            double tripDistance = Double.parseDouble(tokens[6]);
            double puLong = Double.parseDouble(tokens[7]);
            double puLat = Double.parseDouble(tokens[8]);
            double doLong = Double.parseDouble(tokens[9]);
            double doLat = Double.parseDouble(tokens[10]);

            if (!AreaMapper.isValidCoord(puLat, puLong) || !AreaMapper.isValidCoord(doLat, doLong)) {
                return null;
            }

            int pickupCell = AreaMapper.getTransformedCellID(puLat, puLong);
            int dropoffCell = AreaMapper.getTransformedCellID(doLat, doLong);

            NYTDQEvent event = new NYTDQEvent();
            event.vendorId = vendorId;
            event.pickupCell = pickupCell;
            event.dropoffCell = dropoffCell;
            event.tripDistance = tripDistance;
            
            return event;

        } catch (AreaMapper.OutOfGridException | NumberFormatException e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return "NYTDQEvent{" +
                "vendorId='" + vendorId + '\'' +
                ", pickupCell=" + pickupCell +
                ", dropoffCell=" + dropoffCell +
                ", tripDistance=" + tripDistance +
                '}';
    }
}
