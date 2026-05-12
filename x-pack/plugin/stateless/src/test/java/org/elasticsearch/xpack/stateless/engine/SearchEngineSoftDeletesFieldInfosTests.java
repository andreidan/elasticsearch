/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SoftDeletesDirectoryReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * every soft-delete reallocates the FieldInfos object and every FieldInfo within it,
 * even though the underlying SegmentCoreReaders is reused.
 * When DirectoryReader generations stack (e.g. while
 * older searches are still in flight), K open generations means K copies of every
 * field's metadata sitting in heap.
 */
public class SearchEngineSoftDeletesFieldInfosTests extends ESTestCase {

    public void testSoftUpdateRotatesFieldInfosAndAllFieldInfoObjects() throws IOException {
        Directory dir = newDirectory();
        String softDeletesField = "_soft_deletes";
        IndexWriterConfig iwc = newIndexWriterConfig().setSoftDeletesField(softDeletesField).setMergePolicy(NoMergePolicy.INSTANCE);
        IndexWriter writer = new IndexWriter(dir, iwc);

        // three docs, three fields each
        for (int i = 1; i <= 3; i++) {
            Document doc = new Document();
            doc.add(new StringField("id", "d" + i, Field.Store.YES));
            doc.add(new StringField("payload", "v1", Field.Store.YES));
            doc.add(new NumericDocValuesField("version", 1L));
            writer.addDocument(doc);
        }
        writer.commit();

        // Open the first generation. Capture the SegmentReader, its FieldInfos object,
        // every FieldInfo object, the fieldInfosGen, and the SegmentCoreReaders identity.
        DirectoryReader r1 = new SoftDeletesDirectoryReaderWrapper(DirectoryReader.open(dir), softDeletesField);
        assertEquals(1, r1.leaves().size());

        LeafReader leaf1 = r1.leaves().get(0).reader();
        SegmentReader sr1 = unwrapToSegmentReader(leaf1);
        FieldInfos fis1 = sr1.getFieldInfos();
        long gen1 = sr1.getSegmentInfo().getFieldInfosGen();
        Object core1 = sr1.getCoreCacheHelper().getKey();

        Map<String, FieldInfo> byName1 = new HashMap<>();
        for (FieldInfo fi : fis1) {
            byName1.put(fi.name, fi);
        }
        assertTrue(byName1.containsKey("id"));
        assertTrue(byName1.containsKey("payload"));
        assertTrue(byName1.containsKey("version"));

        // soft-update first document. re-add the doc and stamp the old one with the soft-deletes DV.
        Document updated = new Document();
        updated.add(new StringField("id", "d1", Field.Store.YES));
        updated.add(new StringField("payload", "v2", Field.Store.YES));
        updated.add(new NumericDocValuesField("version", 2L));
        writer.softUpdateDocument(new Term("id", "d1"), updated, new NumericDocValuesField(softDeletesField, 1));
        writer.commit();

        DirectoryReader reopened = DirectoryReader.openIfChanged(r1);
        assertNotNull("expected a new reader after softUpdateDocument", reopened);
        DirectoryReader r2 = new SoftDeletesDirectoryReaderWrapper(reopened, softDeletesField);

        // softUpdateDocument = DV update on the existing segment + addDocument flushed
        // as a new segment on commit, so we now see 2 leaves. The leaf we care about
        // is the original segment, that's where FieldInfos got rotated due to the DV update.
        assertEquals(2, r2.leaves().size());
        SegmentReader segmentReaderAfterSoftUdpate = null;
        for (var ctx : r2.leaves()) {
            SegmentReader sr = unwrapToSegmentReader(ctx.reader());
            if (sr.getCoreCacheHelper().getKey() == core1) {
                segmentReaderAfterSoftUdpate = sr;
                break;
            }
        }
        assertNotNull("could not find the rotated segment in the reopened reader", segmentReaderAfterSoftUdpate);
        FieldInfos fis2 = segmentReaderAfterSoftUdpate.getFieldInfos();
        long gen2 = segmentReaderAfterSoftUdpate.getSegmentInfo().getFieldInfosGen();
        Object core2 = segmentReaderAfterSoftUdpate.getCoreCacheHelper().getKey();

        // new SegmentReader instance (as new generation)
        assertNotSame("SegmentReader should rotate on softUpdateDocument", sr1, segmentReaderAfterSoftUdpate);

        // SegmentCoreReaders is reused (postings/stored-fields/etc. are shared).
        // the bit that _does_ work :)
        assertSame("SegmentCoreReaders must be shared across generations", core1, core2);

        // fieldInfosGen advanced
        assertEquals(-1L, gen1);
        assertTrue("fieldInfosGen must advance on DV update", gen2 > gen1);

        // the baddie: the FieldInfos *object* is a different instance
        assertNotSame("FieldInfos must be a new object after softUpdate", fis1, fis2);

        // Every (singular) FieldInfo *object* is a different instance (even fields that didn't change OH NO :scream:)
        // the entire FieldInfo[] is reallocated per generation, so K open generations
        // means K copies of every field's metadata.
        Map<String, FieldInfo> byName2 = new HashMap<>();
        for (FieldInfo fi : fis2) {
            byName2.put(fi.name, fi);
        }
        for (Map.Entry<String, FieldInfo> e : byName1.entrySet()) {
            FieldInfo other = byName2.get(e.getKey());
            assertNotNull("field " + e.getKey() + " disappeared", other);
            assertNotSame("Wish this assertion woudl fail for '" + e.getKey() + "' ", e.getValue(), other);
        }

        // let's confirm this is a real DV update, not just a cosmetic FieldInfos rotation.
        FieldInfo sdField2 = fis2.fieldInfo(softDeletesField);
        assertNotNull("soft-deletes field should now be registered", sdField2);
        assertTrue("dvGen of soft-deletes field must advance after softUpdateDocument", sdField2.getDocValuesGen() > -1L);

        // sanity on the soft-delete itself
        assertEquals(3, r2.numDocs());     // d1(v2), d2, d3
        assertEquals(4, r2.maxDoc());      // plus the soft-deleted original d1

        IOUtils.close(r2, r1, writer, dir);
    }

    // SoftDeletesDirectoryReaderWrapper wraps a leaf in one of two ways depending on
    // whether it also has hard deletes: SoftDeletesFilterLeafReader (extends FilterLeafReader)
    // or SoftDeletesFilterCodecReader (extends FilterCodecReader). FilterLeafReader.unwrap
    // only handles the first. Walk through both kinds to reach the underlying SegmentReader.
    private static SegmentReader unwrapToSegmentReader(LeafReader r) {
        while (true) {
            if (r instanceof SegmentReader sr) {
                return sr;
            }
            if (r instanceof FilterLeafReader fl) {
                r = fl.getDelegate();
            } else if (r instanceof FilterCodecReader fc) {
                r = fc.getDelegate();
            } else {
                throw new AssertionError("cannot unwrap " + r.getClass());
            }
        }
    }
}
