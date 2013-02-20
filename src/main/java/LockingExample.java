/**
 * Created with IntelliJ IDEA.
 * User: Mike
 * Date: 19/02/2013
 * Time: 14:27
 * To change this template use File | Settings | File Templates.
 */

import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.locks.InterProcessSemaphoreV2;
import com.netflix.curator.framework.recipes.locks.Lease;
import com.netflix.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LockingExample {
    private static final int THREADS = 20;
    private static final int MAX_REQUESTS = 1000;
    private static final int REQUEST_SLEEP = 12;
    private static final int MAX_SEMAPHORES = 10;
    private InterProcessSemaphoreV2 semaphore;

    public static void main(String[] args) throws Exception {
        LockingExample test = new LockingExample();
        test.run();
    }

    private void run() {
        initializeSemaphore();

        long startTime = now();

        ExecutorService execService = Executors.newFixedThreadPool( THREADS );

        try {
            for( int i = 0; i < MAX_REQUESTS; i++ ) {
                execService.execute( new ThrottledRequest( i + 1 ) );
                //sleepQuietly( REQUEST_SLEEP );
            }
        } catch( Exception exception ) {
            exception.printStackTrace();
        } finally {
            execService.shutdown();
        }

        while( ! execService.isTerminated() ) {}

        long duration = now() - startTime;
        log( "Total Duration=[" + duration + "]" );
    }

    private void initializeSemaphore() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry( 1000, 3 );

        try {
            CuratorFramework client = CuratorFrameworkFactory.newClient( "127.0.0.1:2181", retryPolicy );
            client.start();
            semaphore = new InterProcessSemaphoreV2( client, "/gateway/FDIAus", MAX_SEMAPHORES );

        } catch( Exception e ) {
            e.printStackTrace();
        }

    }

    private Lease acquire( InterProcessSemaphoreV2 semaphore, int id ) {
        try {
            long start = now();

            Lease lease = semaphore.acquire();

            long duration = now() - start;
            log( "+ acquire { id=[" + id + "], duration=[" + duration + "] }" );

            return lease;
        } catch ( Exception e ) {
            e.printStackTrace();
        }

        return null;
    }

    private void release( InterProcessSemaphoreV2 semaphore, Lease lease, int id ) {
        long start = now();

        semaphore.returnLease( lease );

        long duration = now() - start;
        log( "- release { id=[" + id + "], duration=[" + duration + "] }" );
    }

    private long now() {
        return System.currentTimeMillis();
    }

    private void sleepQuietly( long timeInMillis ) {
        try{
            Thread.sleep( timeInMillis );
        } catch( InterruptedException e ) {
            e.printStackTrace();
        }
    }

    private static void log( String message ) {
        System.out.println( message );
    }

    private class ThrottledRequest implements Runnable {
        int id;

        private ThrottledRequest( int id ) {
            this.id = id;
        }

        public void run() {
            Lease lease = null;

            try {
                long start = now();

                lease = acquire( semaphore, id );
                //sleepQuietly( 500 );
                release( semaphore, lease, id );

                long duration = now() - start;
                log( "> request { id=[" + id + "], duration=[" + duration + "] }" );

            } catch( Exception e ) {
                e.printStackTrace();
            }
            finally {

            }
        }
    }
}
