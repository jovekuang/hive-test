package com.jove.ql.udf.generic;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.TaskExecutionException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;

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

    private transient PrimitiveObjectInspector startOI = null;

    private transient PrimitiveObjectInspector endOI = null;

    private transient PrimitiveObjectInspector incrementOI = null;

    @Override
    public void close() throws HiveException {
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length != 3) {
            throw new UDFArgumentException("custom_explode() takes 3 parameters");
        }

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        for(int i = 0; i < args.length; i++) {
            if (args[i].getCategory() != ObjectInspector.Category.PRIMITIVE
                    && ((PrimitiveObjectInspector) args[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT) {
                throw new UDFArgumentException("custom_explode() takes 3 positive integers as parameters");
            }
        }

        // input inspectors
        startOI = (PrimitiveObjectInspector) args[0];
        endOI = (PrimitiveObjectInspector) args[1];
        incrementOI = (PrimitiveObjectInspector) args[2];

        fieldNames.add("col1");
        fieldNames.add("col2");
        fieldNames.add("result_col");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
                fieldOIs);
    }

    private transient final Object[] forwardListObj = new Object[3];

    @Override
    public void process(Object[] o) throws HiveException {
        if(o == null || o.length != 3){
            throw new TaskExecutionException("custom_explode() takes 3 positive integers as parameters");
        }
        else {
            Integer start = 0;
            Integer end = 0;
            Integer increment = 0;

            try {
                start = (Integer)startOI.getPrimitiveJavaObject(o[0]);
                end = (Integer)endOI.getPrimitiveJavaObject(o[1]);
                increment = (Integer)incrementOI.getPrimitiveJavaObject(o[2]);
            } catch (ClassCastException e) {
                throw new TaskExecutionException("custom_explode() takes 3 positive integers as parameters");
            }

            if(start > end || start < 0 || end < 0 || increment < 0) {
                throw new TaskExecutionException("custom_explode() takes 3 positive integers as parameters and the first parameter must be less than the second parameter."
                        +start+":"+end+":"+increment+" doesn't meet the criteria");
            }
            else {
                for (int newColumnValue = start;
                     newColumnValue <= end;
                     newColumnValue+=increment) { // more error handling on invalid inputs
                    forwardListObj[0] = start; // first column
                    forwardListObj[1] = end; // second column
                    forwardListObj[2] = newColumnValue; // third (new) column
                    forward(forwardListObj);
                }
            }
        }
    }

    @Override
    public String toString() {
        return "custom_explode";
    }
}
