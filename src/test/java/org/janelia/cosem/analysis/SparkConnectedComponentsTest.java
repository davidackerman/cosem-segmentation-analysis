package org.janelia.cosem.analysis;

import static org.junit.Assert.*;

import java.io.IOException;

import org.janelia.cosem.analysis.SparkConnectedComponents;
import org.junit.Test;

public class SparkConnectedComponentsTest {
    
    @Test
    public void testConnectedComponentsN5() throws IOException {
	SparkConnectedComponents.standardConnectedComponentAnalysisWorkflow("shapes", TestHelper.testN5Locations, null, TestHelper.tempN5Locations, "_cc", 0, 1, false, false);	
	SparkConnectedComponents.standardConnectedComponentAnalysisWorkflow("planes", TestHelper.testN5Locations, null, TestHelper.tempN5Locations, "_cc", 0, 1, false, false);

	assertTrue(TestHelper.validationAndTestN5sAreEqual("shapes_cc"));
	assertTrue(TestHelper.validationAndTestN5sAreEqual("planes_cc"));
    }
    
    
    @Test
    public void testConnectedComponentsZarr() throws IOException {
	SparkConnectedComponents.standardConnectedComponentAnalysisWorkflow("shapes", TestHelper.testZarrLocations, null, TestHelper.tempZarrLocations, "_cc", 0, 1, false, false);	
	SparkConnectedComponents.standardConnectedComponentAnalysisWorkflow("planes", TestHelper.testZarrLocations, null, TestHelper.tempZarrLocations, "_cc", 0, 1, false, false);

	assertTrue(TestHelper.validationAndTestZarrsAreEqual("shapes_cc"));
	assertTrue(TestHelper.validationAndTestZarrsAreEqual("planes_cc"));
    }

}
