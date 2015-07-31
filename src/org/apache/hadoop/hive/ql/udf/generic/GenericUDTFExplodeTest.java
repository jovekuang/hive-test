package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: jovek
 * Date: 7/30/15
 * Time: 8:45 PM
 * To change this template use File | Settings | File Templates.
 */
public class GenericUDTFExplodeTest {

    @Test
    public void testGenericUDTFExplode() {

        // set up the models we need
        GenericUDTFExplode genericUDTFExplode = new GenericUDTFExplode();
        ObjectInspector[] inputOI = new ObjectInspector[1];
        inputOI[0] = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);

        try {
            genericUDTFExplode.initialize(inputOI);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        List<String> l1 = new ArrayList<String>();
        l1.add("r1-c1");
        List<String> l2 = new ArrayList<String>();
        l2.add("r2-c1");
        List<String> l3 = new ArrayList<String>();
        l3.add("r3-c1");

        Object[] input = {l1, l2, l3};
        try {
            genericUDTFExplode.process(input);
        } catch (HiveException e) {
            e.printStackTrace();
        }

        // test the results
    }
}
