package com.atguigu.mapreduce.b_hadoopserialization.flowsum2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {
    private Long phone;
    private Long upFlow;
    private Long downFlow;

    public FlowBean() {
    }

    public Long getPhone() {
	return phone;
    }

    public void setPhone(Long phone) {
	this.phone = phone;
    }

    public Long getUpFlow() {
	return upFlow;
    }

    public void setUpFlow(Long upFlow) {
	this.upFlow = upFlow;
    }

    public Long getDownFlow() {
	return downFlow;
    }

    public void setDownFlow(Long downFlow) {
	this.downFlow = downFlow;
    }

    @Override
    public void write(DataOutput out) throws IOException {
	out.writeLong(phone);
	out.writeLong(upFlow);
	out.writeLong(downFlow);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
	this.phone = in.readLong();
	this.upFlow = in.readLong();
	this.downFlow = in.readLong();
    }

    @Override
    public String toString() {
	return phone + "\t" +
		upFlow + "\t" +
		downFlow;
    }
}
