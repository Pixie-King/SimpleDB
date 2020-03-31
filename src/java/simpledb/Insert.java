package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private TransactionId t;
    private OpIterator child;
    private int tableId;
    private int num;
    private TupleDesc td;
    private Tuple tuple;
    private boolean called;
    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        if(!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId)))throw new DbException("TupleDesc " +
                "of child differs from table into which we are to insert");
        this.t = t;
        this.child = child;
        this.tableId = tableId;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE},new String[]{"number"});
        this.tuple = new Tuple(td);
        this.num = 0;
        this.called = false;
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        super.open();
    }

    public void close() {
        super.close();
        child.close();
        called = false;
        num = 0;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        called = false;
        num = 0;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(called)return null;
        called = true;
        while(child.hasNext()){
            try {
                Database.getBufferPool().insertTuple(t,tableId,child.next());
                num ++;
            } catch (IOException e) {
                e.printStackTrace();
            }
            }
        tuple.setField(0,new IntField(num));
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        OpIterator[] children = new OpIterator[1];
        children[0] = child;
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child = children[0];
    }
}
