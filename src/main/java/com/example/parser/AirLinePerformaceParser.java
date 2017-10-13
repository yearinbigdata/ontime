package com.example.parser;

import org.apache.hadoop.io.Text;

public class AirLinePerformaceParser {
	private int year;
	private int month;
	private String uniqueCarrier;
	
	private int arriveDelayTime;
	private int departureDelayTime;
	
	private int distance;
	
	private boolean arriveDelayAvaiable = true;
	private boolean departureDelayAvaiable = true;
	private boolean distanceAvaiable = true;
	
	
	public AirLinePerformaceParser(Text text) {
		
		String[] columns = text.toString().split(",");
		
		year = Integer.parseInt(columns[0]);
		month = Integer.parseInt(columns[1]);
		
		uniqueCarrier = columns[8];
		
		if (!columns[15].equals("NA")) {
			departureDelayTime = Integer.parseInt(columns[15]);
		} else {
			departureDelayAvaiable = false;
		}
		
		if (!columns[14].equals("NA")) {
			arriveDelayTime = Integer.parseInt(columns[14]);
		} else {
			arriveDelayAvaiable = false;
		}		
		
		if (!columns[18].equals("NA")) {
			distance = Integer.parseInt(columns[18]);
		} else {
			distanceAvaiable = false;
		}
	}
	
	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getArriveDelayTime() {
		return arriveDelayTime;
	}

	public void setArriveDelayTime(int arriveDelayTime) {
		this.arriveDelayTime = arriveDelayTime;
	}

	public int getDepartureDelayTime() {
		return departureDelayTime;
	}

	public void setDepartureDelayTime(int departureDelayTime) {
		this.departureDelayTime = departureDelayTime;
	}

	public int getDistance() {
		return distance;
	}

	public void setDistance(int distance) {
		this.distance = distance;
	}

	public boolean isArriveDelayAvaiable() {
		return arriveDelayAvaiable;
	}

	public void setArriveDelayAvaiable(boolean arriveDelayAvaiable) {
		this.arriveDelayAvaiable = arriveDelayAvaiable;
	}

	public boolean isDepartureDelayAvaiable() {
		return departureDelayAvaiable;
	}

	public void setDepartureDelayAvaiable(boolean departureDelayAvaiable) {
		this.departureDelayAvaiable = departureDelayAvaiable;
	}

	public boolean isDistanceAvaiable() {
		return distanceAvaiable;
	}

	public void setDistanceAvaiable(boolean distanceAvaiable) {
		this.distanceAvaiable = distanceAvaiable;
	}

	public String getUniqueCarrier() {
		return uniqueCarrier;
	}

	public void setUniqueCarrier(String uniqueCarrier) {
		this.uniqueCarrier = uniqueCarrier;
	}

	

}
