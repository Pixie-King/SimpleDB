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
    Map<Field,Integer> groupPairMap;
    Map<Field,List<Integer>> countForAve;
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
        groupPairMap = new HashMap<>();
        countForAve = new HashMap<>();
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
        Field groupValue = this.gbfield == NO_GROUPING ? null : tup.getField(this.gbfield);
        int aggregateValue = Integer.parseInt(tup.getField(afield).toString());
        switch (what) {
            case SUM:
                int sum = aggregateValue;
                for (Map.Entry<Field, Integer> entry : groupPairMap.entrySet()) {
                    if(entry.getKey().equals(groupValue)){
                        sum += entry.getValue();
                    }
                }
                groupPairMap.put(groupValue, sum);
                break;
            case MIN:
                int min = aggregateValue;
                for (Map.Entry<Field, Integer> entry : groupPairMap.entrySet()) {
                    if(entry.getKey().equals(groupValue)){
                        if(min > entry.getValue())min = entry.getValue();
                    }
                }
                groupPairMap.put(groupValue, min);
                break;
            case MAX:
                int max = aggregateValue;
                for (Map.Entry<Field, Integer> entry : groupPairMap.entrySet()) {
                    if(entry.getKey().equals(groupValue)){
                        if(max < entry.getValue())max = entry.getValue();
                    }
                }
                groupPairMap.put(groupValue, max);
                break;
            case AVG:
                if(!countForAve.containsKey(groupValue)){
                    countForAve.put(groupValue,new ArrayList<>());
                    countForAve.get(groupValue).add(aggregateValue);
            }
                else countForAve.get(groupValue).add(aggregateValue);
                List<Integer> integers = countForAve.get(groupValue);
                int sum1 = 0;
                for (int i = 0; i < integers.size(); i++) {
                    sum1 += integers.get(i);
                }
                groupPairMap.put(groupValue,sum1/integers.size());
                break;
            case COUNT:
                if(!groupPairMap.containsKey(groupValue)){
                    groupPairMap.put(groupValue,1);
                }
                else groupPairMap.put(groupValue,groupPairMap.get(groupValue) + 1);
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
        TupleDesc td1 = new TupleDesc(new Type[]{gbfieldtype,Type.INT_TYPE},
                new String[]{"groupVal","aggregateVal"});
        TupleDesc td2 = new TupleDesc(new Type[]{Type.INT_TYPE},
                new String[]{"aggregateVal"});
        for(Map.Entry<Field, Integer> entry : groupPairMap.entrySet()) {
                if(entry.getKey() !=null){
                Tuple tuple = new Tuple(td1);
                tuple.setField(0,entry.getKey());
                tuple.setField(1,new IntField(entry.getValue()));
                tupleList.add(tuple);
            }
            else{
                Tuple tuple = new Tuple(td2);
                tuple.setField(0,new IntField(entry.getValue()));
                tupleList.add(tuple);
            }
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
