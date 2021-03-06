package simpledb;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    private int numPages;

    private HashMap<PageId,Page> pages;

    private LockManager lockManager;

    /** Bytes per page, including header. */

    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;


    public class Lock{
        public TransactionId tid;
        public boolean isShared;

        public Lock(TransactionId tid,boolean isShared){
            this.tid = tid;
            this.isShared = isShared;
        }
    }

    public class LockManager{
        ConcurrentHashMap<PageId,ArrayList<BufferPool.Lock>> lockMap; //学习多线程时博客中提到HashMap多线程不安全，建议采用ConcurrentHashMap

        public LockManager(){
            lockMap = new ConcurrentHashMap<PageId,ArrayList<BufferPool.Lock>>();
        }

        public synchronized boolean acquireLock(PageId pid,TransactionId tid,boolean isShared) {
            //此页无锁则添加一个锁
            if (lockMap.get(pid) == null) {
                Lock lock = new Lock(tid, isShared);
                ArrayList<Lock> locks = new ArrayList<BufferPool.Lock>();
                locks.add(lock);
                lockMap.put(pid, locks);
                return true;
            }

            ArrayList lockList = lockMap.get(pid);

            /**
             * 共享锁【S锁】又称读锁，若事务T对数据对象A加上S锁，则事务T可以读A但不能修改A，
             * 其他事务只能再对A加S锁，而不能加X锁，直到T释放A上的S锁。这保证了其他事务可以读A，
             * 但在T释放A上的S锁之前不能对A做任何修改。
             * 排他锁【X锁】又称写锁。若事务T对数据对象A加上X锁，事务T可以读A也可以修改A，
             * 其他事务不能再对A加任何锁，直到T释放A上的锁。这保证了其他事务在T释放A上的锁之前不能再读取和修改A。
             *
             * 由此可知可以分为以下几种情况
             * 一、本事务在此存有锁
             *   若是当前需要的锁，则获取成功，无需添加
             *   若当前为排他锁，需要共享锁，则无需添加，因为共享锁代表读取权限，排他锁代表读写权限
             *   若当前为共享锁，需要排他锁，分为两种，若只有一个锁，升格为排他锁；否则申请失败
             * 二、本事务在此不持有锁
             *   若在此有其他的排他锁，则此事务不能在此申请锁
             *   若在此有其他锁的共享锁，则此事务可以添加一个共享锁，但不能添加排他锁
             */
            for (Object o : lockList) {
                Lock lock = (Lock) o;
                if (lock.tid == tid) {
                    if (lock.isShared == isShared)
                        return true;
                    if (!lock.isShared)
                        return true;
                    if (lockList.size() == 1) {
                        lock.isShared = false;
                        return true;
                    } else return false;
                }
            }
            if (lockList.size() == 1 && !((Lock) lockList.get(0)).isShared) {
                return false;
            }
            if (isShared) {
                    Lock lock = new Lock(tid, true);
                    lockList.add(lock);
                    lockMap.put(pid, lockList);
                    return true;
            }
            return false;
        }

        public synchronized boolean releaseLock(PageId pid,TransactionId tid){
            if(lockMap.get(pid) == null){
                return false;
            }
            ArrayList<Lock> locks = lockMap.get(pid);
            for(int i=0;i<locks.size();++i){
                Lock lock = locks.get(i);
                if(lock.tid == tid){
                    locks.remove(lock);
                    if(locks.size() == 0)
                        lockMap.remove(pid);
                    return true;
                }
            }
            return false;
        }
        public synchronized boolean holdsLock(PageId pid,TransactionId tid){
            if(lockMap.get(pid) == null){
                return false;
            }
            ArrayList<Lock> locks =  lockMap.get(pid);
            for (int i = 0; i < locks.size(); i++) {
                if(locks.get(i).tid == tid)return true;
            }
            return false;
        }
    }
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
       this.numPages = numPages;
           pages = new HashMap<>();
           lockManager = new LockManager();
    }

    public static int getPageSize() {
      return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException{
        boolean isShared;
        if(perm == Permissions.READ_ONLY){
            isShared = true;
        }
        else isShared = false;
        boolean lockAcquired = false;
        long start = System.currentTimeMillis();
        long timeout = new Random().nextInt(2000) + 1000;
        while(!lockAcquired){
            long now = System.currentTimeMillis();
            if(now-start > timeout){
                throw new TransactionAbortedException();
            }
            lockAcquired = lockManager.acquireLock(pid,tid,isShared);
        }

        lockManager.acquireLock(pid,tid,isShared);
        if(tid == null)throw new TransactionAbortedException();
        if (this.pages.containsKey(pid))return pages.get(pid);
        if(pages.size()>= numPages)evictPage();
        Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        pages.put(pid,page);
        if(holdsLock(tid,pid)){

        }
        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        lockManager.releaseLock(pid,tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdsLock(p,tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        if(commit)flushPages(tid);
        else{
            for (PageId pid : pages.keySet()) {
                Page page = pages.get(pid);
                if (page.isDirty() == tid) {
                    int tabId = pid.getTableId();
                    DbFile file =  Database.getCatalog().getDatabaseFile(tabId);
                    Page pageFromDisk = file.readPage(pid);
                    pages.put(pid, pageFromDisk);
                }
            }
        }
        for(PageId pid:pages.keySet()){
            if(holdsLock(tid,pid))
                releasePage(tid,pid);
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> page = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        for (Page p:page) {
            p.markDirty(true,tid);
            pages.put(p.getId(),p);
        }
    }
    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> page = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t);
        for (Page p:page) {
            p.markDirty(true,tid);
            pages.put(p.getId(),p);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for(Map.Entry<PageId, Page> entry : pages.entrySet()) {
            flushPage(entry.getKey());
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.

        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        pages.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        if(pages.get(pid).isDirty() != null)
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(pages.get(pid));
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        for(Map.Entry<PageId, Page> entry : pages.entrySet()) {
            if(entry.getValue().isDirty() == tid)flushPage(entry.getKey());
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        boolean allDirty = true;
        for(Map.Entry<PageId, Page> entry : pages.entrySet()) {
            if(entry.getValue().isDirty() != null)continue;
            try {
                allDirty = false;
                flushPage(entry.getKey());
            } catch (IOException e) {
                e.printStackTrace();
            }
            discardPage(entry.getKey());
            break;
        }
        if(allDirty)throw new DbException("All pages are dirty");
    }
}
