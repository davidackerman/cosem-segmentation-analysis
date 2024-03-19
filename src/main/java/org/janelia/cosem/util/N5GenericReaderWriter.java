package org.janelia.cosem.util;

import java.io.IOException;

import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.universe.N5Factory;

public class N5GenericReaderWriter {
    public static N5Reader N5GenericReader(final String n5Path) throws IOException {
	N5Reader n5;
	N5Factory factory = new N5Factory();
	n5 = factory.openReader(n5Path);
	return n5;
    }

    public static N5Writer N5GenericWriter(final String n5Path) throws IOException {
	N5Writer n5;
	N5Factory factory = new N5Factory();
	n5 = factory.openWriter(n5Path);
	return n5;
    }
}
