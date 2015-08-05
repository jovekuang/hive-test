package com.jove.ql.udf.generic;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.TaskExecutionException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: jovek
 * Date: 7/31/15
 * Time: 11:02 PM
 * To change this template use File | Settings | File Templates.
 */

@Description(name = "custom_explode",
        value = "_FUNC_(a) - explode a range of integers")
public class GenericUDTFCustomExplode extends GenericUDTF {

    private transient ObjectInspector inputOI = null;

    @Override
    public void close() throws HiveException {
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length != 1) {
            throw new UDFArgumentException("custom_explode() takes only one argument");
        }

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        switch (args[0].getCategory()) {
            case LIST:
                inputOI = args[0];
                fieldNames.add("col1");
                fieldNames.add("col2");
                fieldNames.add("col3");
                fieldOIs.add(((ListObjectInspector)inputOI).getListElementObjectInspector());
                fieldOIs.add(((ListObjectInspector)inputOI).getListElementObjectInspector());
                fieldOIs.add(((ListObjectInspector)inputOI).getListElementObjectInspector());
                break;
            default:
                throw new UDFArgumentException("custom_explode() takes an array of 3 elements as a parameter");
        }

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
                fieldOIs);
    }

    private transient final Object[] forwardListObj = new Object[3];

    @Override
    public void process(Object[] o) throws HiveException {
        switch (inputOI.getCategory()) {
            case LIST:
                ListObjectInspector listOI = (ListObjectInspector)inputOI;
                List<?> list = listOI.getList(o[0]);
                if (list == null || list.size() != 3) {
                    return; // we may want to throw an exception to let the user know that custom_explode takes an array of 3 elements
                }
                for (int newColumnValue = NumberUtils.toInt(list.get(0).toString());
                       newColumnValue <= NumberUtils.toInt(list.get(1).toString());
                       newColumnValue+=NumberUtils.toInt(list.get(2).toString())) { // more error handling on invalid inputs
                    forwardListObj[0] = list.get(0); // first column
                    forwardListObj[1] = list.get(1); // second column
                    forwardListObj[2] = new Text(Integer.toString(newColumnValue)); // third (new) column
                    forward(forwardListObj);
                }
                break;
            default:
                throw new TaskExecutionException("custom_explode() takes an array of 3 elements as a parameter");
        }
    }

    @Override
    public String toString() {
        return "custom_explode";
    }
}
