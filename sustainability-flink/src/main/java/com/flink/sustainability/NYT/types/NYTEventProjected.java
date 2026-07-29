package com.flink.sustainability.NYT.types;

import com.flink.sustainability.NYT.function.*;


public class NYTEventProjected {
    public String medallion;
    public int pickupCellWE;
    public int pickupCellNS;
    public int dropOffCellWE;
    public int dropOffCellNS;
    public double fareAmount;
    public double tipAmount;

    public NYTEventProjected() {}

    public NYTEventProjected(String medallion, int pickupCellWE, int pickupCellNS, 
                             int dropOffCellWE, int dropOffCellNS, double fareAmount, double tipAmount) {
        this.medallion = medallion;
        this.pickupCellWE = pickupCellWE;
        this.pickupCellNS = pickupCellNS;
        this.dropOffCellWE = dropOffCellWE;
        this.dropOffCellNS = dropOffCellNS;
        this.fareAmount = fareAmount;
        this.tipAmount = tipAmount;
    }

    public int getPickupCell() {
        return pickupCellNS * (AreaMapper.MAX_CELL - 1) + pickupCellWE;
    }

    public int getDropoffCell() {
        return dropOffCellNS * (AreaMapper.MAX_CELL - 1) + dropOffCellWE;
    }
}
