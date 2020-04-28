package com.taosdata.tools.bean;

import com.taosdata.tools.WeichaiTypeName;

@WeichaiTypeName("location")
public class LocationInfoBean extends WeichaiBean {
    //location
    private double speed;
    private String csq;
    private String geohash;
//    private String address;
    private String districtCode;
    private String direction;
    private String lonOffset;
    private String altitude;
    private String latType;
    private double latitude;
    private String lonType;
    private double longitude;
    private String satekkiteNum;
    private String hdop;
    private String latOffset;
    private String province;
    private String city;
    private String district;
    private String provinceCode;

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public String getCsq() {
        return csq;
    }

    public void setCsq(String csq) {
        this.csq = csq;
    }

    public String getGeohash() {
        return geohash;
    }

    public void setGeohash(String geohash) {
        this.geohash = geohash;
    }

//    public String getAddress() {
//        return address;
//    }

//    public void setAddress(String address) {
//        this.address = address;
//    }

    public String getDistrictCode() {
        return districtCode;
    }

    public void setDistrictCode(String districtCode) {
        this.districtCode = districtCode;
    }

    public String getDirection() {
        return direction;
    }

    public void setDirection(String direction) {
        this.direction = direction;
    }

    public String getLonOffset() {
        return lonOffset;
    }

    public void setLonOffset(String lonOffset) {
        this.lonOffset = lonOffset;
    }

    public String getAltitude() {
        return altitude;
    }

    public void setAltitude(String altitude) {
        this.altitude = altitude;
    }

    public String getLatType() {
        return latType;
    }

    public void setLatType(String latType) {
        this.latType = latType;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public String getLonType() {
        return lonType;
    }

    public void setLonType(String lonType) {
        this.lonType = lonType;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public String getSatekkiteNum() {
        return satekkiteNum;
    }

    public void setSatekkiteNum(String satekkiteNum) {
        this.satekkiteNum = satekkiteNum;
    }

    public String getHdop() {
        return hdop;
    }

    public void setHdop(String hdop) {
        this.hdop = hdop;
    }

    public String getLatOffset() {
        return latOffset;
    }

    public void setLatOffset(String latOffset) {
        this.latOffset = latOffset;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getDistrict() {
        return district;
    }

    public void setDistrict(String district) {
        this.district = district;
    }

    public String getProvinceCode() {
        return provinceCode;
    }

    public void setProvinceCode(String provinceCode) {
        this.provinceCode = provinceCode;
    }

    public String getCityCode() {
        return cityCode;
    }

    public void setCityCode(String cityCode) {
        this.cityCode = cityCode;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    private String cityCode;
    private String status;

}
