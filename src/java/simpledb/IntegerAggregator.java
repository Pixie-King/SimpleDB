package simpledb;

import java.io.InputStream;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    int gbfield;
    Type gbfieldtype;
    int afield;
    Op what;
    Map<Integer,Integer> groupPairMap;
    List<Tuple> tupleList;
    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        groupPairMap = new HashMap();
        tupleList = new ArrayList<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        int groupValue = Integer.parseInt(tup.getField(gbfield).toString());
        int aggregateValue = Integer.parseInt(tup.getField(afield).toString());
        TupleDesc td2 = new TupleDesc(new Type[]{Type.INT_TYPE},
                new String[]{"aggregateVal"});
        if(groupValue == Aggregator.NO_GROUPING) {
            Tuple tuple = new Tuple(td2);
            tuple.setField(0, tup.getField(afield));
            tupleList.add(tuple);
        }
        switch (what) {
            case SUM:
                int sum = aggregateValue;
                for (Map.Entry<Integer, Integer> entry : groupPairMap.entrySet()) {
                    if(entry.getKey() == groupValue){
                        sum += entry.getValue();
                    }
                }
                groupPairMap.put(groupValue, sum);
                break;
            case MIN:
                int min = aggregateValue;
                for (Map.Entry<Integer, Integer> entry : groupPairMap.entrySet()) {
                    if(entry.getKey() == groupValue){
                        if(min > entry.getValue())min = entry.getValue();
                    }
                }
                groupPairMap.put(groupValue, min);
                break;
            case MAX:
                int max = aggregateValue;
                for (Map.Entry<Integer, Integer> entry : groupPairMap.entrySet()) {
                    if(entry.getKey() == groupValue){
                        if(max < entry.getValue())max = entry.getValue();
                    }
                }
                groupPairMap.put(groupValue, max);
                break;
            case AVG:
                int sum1 = aggregateValue;
                int i = 1;
                for (Map.Entry<Integer, Integer> entry : groupPairMap.entrySet()) {
                    if(entry.getKey() == groupValue){
                        sum1 += entry.getValue();
                        i++;
                    }
                }
                groupPairMap.put(groupValue, sum1/i);
                break;
            default:break;
        }
    }


    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        tupleList = new ArrayList();
        TupleDesc td1 = new TupleDesc(new Type[]{Type.INT_TYPE,Type.INT_TYPE},
                new String[]{"groupVal","aggregateVal"});
        for(Map.Entry<Integer, Integer> entry : groupPairMap.entrySet()) {
            Tuple tuple = new Tuple(td1);
            tuple.setField(0,new IntField(entry.getKey()));
            tuple.setField(1,new IntField(entry.getValue()));
            tupleList.add(tuple);
        }
       return new OpIterator() {
           private boolean isOpen;
           Iterator<Tuple> it;
           @Override
           public void open() throws DbException, TransactionAbortedException {
                isOpen = true;
                it = tupleList.iterator();
           }

           @Override
           public boolean hasNext() throws DbException, TransactionAbortedException {
               return isOpen && it.hasNext();
           }

           @Override
           public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
               if(!isOpen) throw new DbException("Not open");
               for(Map.Entry<Integer, Integer> entry : groupPairMap.entrySet()) {
                   //System.out.println(entry.getKey()+" "+entry.getValue());
               }
               return it.next();
           }

           @Override
           public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
           }

           @Override
           public TupleDesc getTupleDesc() {
               return tupleList.get(0).getTupleDesc();
           }

           @Override
           public void close() {
                isOpen = false;
                it = null;
           }
       };
    }

}
