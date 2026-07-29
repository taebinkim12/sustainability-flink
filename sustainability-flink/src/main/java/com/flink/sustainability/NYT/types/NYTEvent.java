package com.flink.sustainability.NYT.types;

import com.flink.sustainability.NYT.function.*;


import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * NYTEvent class, based on TaxiRide schema.
 */
public class NYTEvent {
    private static final int NO_FIELDS = 17;
    private static DateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // 2013-01-01 00:02:00
    public String medallion, hackLicense;
    public Date pickupTS, dropoffTS;
    public Integer tripTimeInSec;
    public Double tripDistance;
    public Integer pickupCell, dropoffCell;
    public String paymentType;
    public Double fare, surcharge, mtaTax, tipAmount, tollsAmount, totalAmount;

    /**
     * @param line the line to be parsed in csv format. E.g.
     *             DFBF A82E CA8F 7059 B89C 3E8B 93DA A377,
     *             CF8604E72D83840FBA1978C2D2FC9CDB,
     *             2013-01-01 00:02:00,
     *             2013-01-01 00:03:00,
     *             60,
     *             0.39,
     *             -73.981544,
     *             40.781475,
     *             -73.979439,
     *             40.784386,
     *             CRD,
     *             3.00,0.50,0.50,0.70,0.00,4.70
     * @return null if there was any error while parsing; a NYTEvent object instead.
     */
    public static NYTEvent parseLine(String line) {
        String[] tokens = line.split(",");

        if (tokens.length != NO_FIELDS) {
            return null;
        }

        try {
            String medallion = tokens[0];
            String hackLicense = tokens[1];
            Date pickupTS = dateFmt.parse(tokens[2]);
            Date dropoffTS = dateFmt.parse(tokens[3]);
            Integer tripTimeInSec = Integer.valueOf(tokens[4]);
            Double tripDistance = Double.valueOf(tokens[5]);
            double puLat = Double.valueOf(tokens[7]);
            double puLong = Double.valueOf(tokens[6]);
            double doLat = Double.valueOf(tokens[9]);
            double doLong = Double.valueOf(tokens[8]);

            Integer pickupCell = AreaMapper.getTransformedCellID(puLat, puLong);
            Integer dropoffCell = AreaMapper.getTransformedCellID(doLat, doLong);

            String paymentType = String.valueOf(tokens[10]);
            Double fare = Double.valueOf(tokens[11]);
            Double surcharge = Double.valueOf(tokens[12]);
            Double mtaTax = Double.valueOf(tokens[13]);
            Double tipAmount = Double.valueOf(tokens[14]);
            Double tollsAmount = Double.valueOf(tokens[15]);
            Double totalAmount = Double.valueOf(tokens[16]);

            NYTEvent tr = new NYTEvent();
            tr.medallion = medallion;
            tr.hackLicense = hackLicense;
            tr.pickupTS = pickupTS;
            tr.dropoffTS = dropoffTS;
            tr.tripTimeInSec = tripTimeInSec;
            tr.tripDistance = tripDistance;
            tr.pickupCell = pickupCell;
            tr.dropoffCell = dropoffCell;
            tr.paymentType = paymentType;
            tr.fare = fare;
            tr.surcharge = surcharge;
            tr.mtaTax = mtaTax;
            tr.tipAmount = tipAmount;
            tr.tollsAmount = tollsAmount;
            tr.totalAmount = totalAmount;
            return tr;

        } catch (AreaMapper.OutOfGridException | ParseException | NumberFormatException e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return "NYTEvent {" +
                "\n\tmedallion = '" + medallion + '\'' +
                ",\n\thackLicense = '" + hackLicense + '\'' +
                ",\n\tpickupTS = " + dateFmt.format(pickupTS) +
                ",\n\tdropoffTS = " + dateFmt.format(dropoffTS) +
                ",\n\ttripTimeInSec = " + tripTimeInSec +
                ",\n\ttripDistance = " + tripDistance +
                ",\n\tpickupCell = '" + pickupCell + '\'' +
                ",\n\tdropoffCell = '" + dropoffCell + '\'' +
                ",\n\tpaymentType = '" + paymentType + '\'' +
                ",\n\tfare = " + fare +
                ",\n\tsurcharge = " + surcharge +
                ",\n\tmtaTax = " + mtaTax +
                ",\n\ttipAmount = " + tipAmount +
                ",\n\ttollsAmount = " + tollsAmount +
                ",\n\ttotalAmount = " + totalAmount +
                "\n}\n";
    }

    @Override
    public int hashCode() {
        int result = pickupTS != null ? pickupTS.hashCode() : 0;
        result = 31 * result + (dropoffTS != null ? dropoffTS.hashCode() : 0);
        result = 31 * result + (pickupCell != null ? pickupCell.hashCode() : 0);
        result = 31 * result + (dropoffCell != null ? dropoffCell.hashCode() : 0);
        return result;
    }
}
