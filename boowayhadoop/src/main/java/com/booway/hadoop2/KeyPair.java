package com.booway.hadoop2;

import org.apache.hadoop.io.WritableComparable;

import javax.net.ssl.KeyStoreBuilderParameters;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by may on 2017/7/16.
 */
public class KeyPair implements WritableComparable<KeyPair> {

    private int year;

    private int hot;

    public KeyPair() {


    }

    public KeyPair(int year, int host) {
        this.year = year;
        this.hot = hot;

    }


    public int getYear() {
        return year;
    }

    public int getHot() {
        return hot;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public void setHot(int hot) {
        this.hot = hot;
    }

    /**
     * Hadoop的数据传递使用rpc协议，序列化
     * @param in
     * @throws IOException
     */
    @Override
    public void write(DataOutput in) throws IOException {
        in.write(year);
        in.write(hot);

    }

    /**
     * Hadoop需要写入到磁盘，这里是反序列化
     * @param out
     * @throws IOException
     */
    @Override
    public void readFields(DataInput out) throws IOException {
        this.year = out.readInt();
        this.hot = out.readInt();
    }

    @Override
    public int compareTo(KeyPair o) {

       int res = Integer.compare(year, o.getYear());

       if(res != 0) {
            return res;

       }

        return Integer.compare(o.getHot(), hot);
    }

    @Override
    public String toString() {
        return year + "\t" + hot;
    }

    @Override
    public int hashCode() {
        return new Integer(year + hot).hashCode();
    }
}
