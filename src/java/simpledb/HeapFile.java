package simpledb;

import jdk.nashorn.internal.runtime.arrays.ArrayLikeIterator;

import java.io.*;
import java.util.*;

import static jdk.nashorn.internal.ir.debug.ObjectSizeCalculator.getObjectSize;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    File f;
    TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td)  {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid){
        int len = BufferPool.getPageSize();
        int offset = pid.getPageNumber() * len;
        byte[]data = new byte[len];
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(f,"r");
            randomAccessFile.read(data,offset,len);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        HeapPage heapPage = null;
        try {
            heapPage = new HeapPage((HeapPageId) pid,data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return heapPage;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int)f.length() / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid){
        return new DbFileIterator() {
            private int pid = 0;
            private BufferPool bufferPool = Database.getBufferPool();
            private HeapPage page;
            private Iterator<Tuple> it;
            private boolean isOpen = false;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                isOpen = true;
                getPage(pid++);
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return isOpen && pid < numPages() || (pid == numPages() && it.hasNext());
            }

            private boolean getPage(int pid) throws TransactionAbortedException, DbException {
                if (!isOpen) throw new DbException("closed");
                page = (HeapPage) bufferPool.getPage(tid, new HeapPageId(getId(), pid), Permissions.READ_ONLY);
                if (page == null) return false;
                it = page.iterator();
                return true;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!isOpen || it == null)
                    throw new NoSuchElementException();
                if (!it.hasNext()) {
                    getPage(pid++);
                }
                return it.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            @Override
            public void close() {
                pid = 0;
                isOpen = false;
                page = null;
                it = null;
            }
        };
    }

}

