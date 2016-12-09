package edu.indiana.p538;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by ladyl on 11/19/2016.
 */
public class Proxy implements Runnable {

    /* FIELDS */
    private int port = AppConstants.PROXY_PORT;
    private ServerSocketChannel serverSocketChannel;
    private Selector selector;
    private ProxyWorker worker;
    private int expectedSequenceNumber=0;
    private ByteBuffer readBuf = ByteBuffer.allocate(8208); //buffer equal to 2 pipe messages; can adjust as necessary
    private int pipes;
    private ArrayList<SocketChannel> freePipes;

    private BlockingQueue<ProxyEvents> pendingEvents = new ArrayBlockingQueue<>(50);
    private ConcurrentHashMap<Integer,SocketChannel> connectionChannelMap = new ConcurrentHashMap<>();
    //this map is to map connection IDs with the list of data
    //Renamed this map to avoid confusion
    private ConcurrentHashMap<Integer,Integer> expectedSequenceList=new ConcurrentHashMap<>();
    //this hashmap is of the form connID->{seqId->data}
    private ConcurrentHashMap<Integer,ConcurrentSkipListMap<Integer,byte[]>> connectionDataList = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Integer,ArrayList<byte[]>> responseDataList = new ConcurrentHashMap<>();
    public Proxy(int port, ProxyWorker worker, int numPipes) throws IOException{
        this.port = port;
        this.serverSocketChannel = ServerSocketChannel.open();
        this.worker = worker;
        //initiate the selector
        this.serverSocketChannel.socket().bind(new InetSocketAddress(port));
        this.serverSocketChannel.configureBlocking(false);
        this.selector = Selector.open();
        this.pipes=numPipes;
        this.freePipes =new ArrayList<>(pipes);
        //this says we're looking for new connections to the given port
        this.serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    @Override
    public void run() {
        while(true){
            try {
                Iterator<ProxyEvents> iter = this.pendingEvents.iterator();
                while(iter.hasNext()){
                    ProxyEvents event = iter.next();
                    this.pendingEvents.remove(event);
                    switch (event.getType()){
                        case ProxyEvents.WRITING:
                            SocketChannel connectChannel=this.connectionChannelMap.get(event.getConnId());
                            if(connectChannel!=null) {
                                SelectionKey key = connectChannel.keyFor(this.selector);
                                key.interestOps(event.getOps());
                                if(freePipes.contains(connectChannel)){
                                    freePipes.remove(connectChannel);
                                }
                            }
                            break;
                        case ProxyEvents.CONNECTING:
                            connectChannel=this.connectionChannelMap.get(event.getConnId());
                            //Need to double check the register call.
                            connectChannel.register(this.selector,event.getOps(),event.getConnId());
                            break;
                        case ProxyEvents.ENDING:
                            connectChannel=this.connectionChannelMap.get(event.getConnId());
                            if(responseDataList.containsKey(event.getConnId())){
                                System.out.println("It has key");
                            }
                            this.responseDataList.remove(event.getConnId());
                            SelectionKey endKey = connectChannel.keyFor(this.selector);
                            expectedSequenceNumber=0;
                            connectChannel.close();
                            //endKey.cancel();
                            break;
                        default: break;
                    }
                }

                this.selector.select();
                Iterator<SelectionKey> keys = this.selector.selectedKeys().iterator();
                while(keys.hasNext()){
                    SelectionKey key = keys.next();
                    keys.remove();
                    if(key.isValid()){
                        if(key.isConnectable()){
                            this.completeConnection(key);
                        }
                        if(key.isAcceptable()){
                            //accept key
                            this.accept(key);
                        }else if(key.isReadable()){
                            //read the key
                            this.read(key);

                        }else if(key.isWritable()){
                            //write to the key
                            this.write(key);
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    private void accept(SelectionKey key) throws IOException{
        ServerSocketChannel servCh = (ServerSocketChannel) key.channel();

        SocketChannel sockCh = servCh.accept();
        sockCh.configureBlocking(false);
        freePipes.add(sockCh);
        //tells the selector we want to know when data is available to be read
        sockCh.register(this.selector, SelectionKey.OP_READ);
    }

    private void read(SelectionKey key) throws IOException{
        SocketChannel sockCh = (SocketChannel) key.channel();
        //clear the buffer. if we've reached this point again we've already passed data on
        this.readBuf.clear();
        //TODO DEBUG
        if(key.attachment()==null && freePipes.contains(key.channel())) {
            freePipes.remove(key.channel());
        }
        int numRead;
        try{
            numRead = sockCh.read(this.readBuf);
        }catch (IOException e) {
            //entering here means the remote has forced the connection closed
            key.cancel();
            sockCh.close();
            return;
        }

        if(numRead == -1) {
            //socket shut down cleanly. cancel channel
            System.out.println("Closed socket");

          //TODO DEBUG
            key.channel().close();
            key.cancel();
            return;
        }

        // Since we're not attaching anything to the LP socket channel, attachment would be empty
        //hand to worker thread only if the read is called from the LP socket
        if(key.attachment()==null) {
            this.worker.processData(this, sockCh, this.readBuf.array(), numRead);
            key.interestOps(SelectionKey.OP_READ);

        }
        else{

            int connectionId=(int)key.attachment();

            if(!freePipes.isEmpty()){
                if(this.responseDataList.containsKey(connectionId)){
                    ArrayList<byte[]> dataMessages=this.responseDataList.get(connectionId);
                    byte[] dataMsg=PacketAnalyzer.generateDataMessage(readBuf,connectionId,expectedSequenceNumber,numRead);
                    expectedSequenceNumber+=1;
                    dataMessages.add(dataMsg);
                    this.responseDataList.put(connectionId,dataMessages);
                }
                else{
                    ArrayList<byte[]> dataMessages=new ArrayList<>();
                    byte[] dataMsg=PacketAnalyzer.generateDataMessage(readBuf,connectionId,expectedSequenceNumber,numRead);
                    expectedSequenceNumber+=1;
                    dataMessages.add(dataMsg);
                    this.responseDataList.put(connectionId,dataMessages);
                }

            }
        }
    }

    protected  void send(int connInfo, byte[] data,int seqId){
        //add it to the buffer queue, send on as we can
        //Null check needed

        //Pull the data based on the connection ID

        //Here the data comes in first and so we're expecting the SYN packet first
        if(!expectedSequenceList.containsKey(connInfo)){
            expectedSequenceList.put(connInfo,0);
        }
        else{
            int expectedSeq=expectedSequenceList.get(connInfo);
            //If the expected seq is what comes in, we increment the expectedseq number
            if(expectedSeq==seqId){
                expectedSequenceList.put(connInfo,seqId+1);
                //Need to read data into buffer here and raise ProxyDataEvent
                this.pendingEvents.add(new ProxyEvents(data, connInfo, ProxyEvents.WRITING,SelectionKey.OP_WRITE,seqId));
            }
        }
        if(connectionDataList.containsKey(connInfo)){
            //Buffer the data
            ConcurrentSkipListMap<Integer,byte[]> dataMap= connectionDataList.get(connInfo);
            dataMap.put(seqId,data);
            connectionDataList.put(connInfo,dataMap);
            NavigableSet<Integer> availSequences=dataMap.keySet();
            int expectedSeq=expectedSequenceList.get(connInfo);
            while(availSequences.contains(expectedSeq)&&dataMap.get(expectedSeq)!=null){
                //This ensures that expectedSeq does not point to a sequence number that already exists in the map
                expectedSeq+=1;
            }
            expectedSequenceList.put(connInfo,expectedSeq);

        }
        else{
            ConcurrentSkipListMap<Integer,byte[]> dataMap=new ConcurrentSkipListMap<>();
            dataMap.put(seqId,data);
            connectionDataList.put(connInfo,dataMap);
        }
    }

    protected void establishConn(InetSocketAddress msgInfo, byte[] data, int connId){
        //add to event queue; create connection as possible
        try {
            SocketChannel serverChannel = SocketChannel.open();
            serverChannel.configureBlocking(false);
            // Kick off connection establishment
            //I've temporarily added port as the key to this map. Should we think of making this the connectionID?
            connectionChannelMap.put(connId,serverChannel);
            //Since it is a SYN packet, we effectively have received the packet with sequence number 0
            //Now we expect for sequence number 1
            if(!expectedSequenceList.containsKey(connId)){
                expectedSequenceList.put(connId,1);
            }

            //So, there are already other out of order packets captured for this connId
            //Since this is the initial SYN packet, we commence sending the data
            else{
                //TODO Retrieve the out of order data from connectionDatalist and keep sending packets
            }
            //OP_CONNECT is getting masked by the call from ProxyWorker. Safe to listen to OP_WRITE here
            this.pendingEvents.add(new ProxyEvents( data, connId,ProxyEvents.CONNECTING,SelectionKey.OP_CONNECT,-1));
            //serverChannel.connect(msgInfo);
            //No point in waking up here as there may not be enough data to write into the channel
            //this.selector.wakeup();
            serverChannel.connect(msgInfo);

        }
        catch(IOException e){
            e.printStackTrace();
        }
    }

    protected void sendFin(int connId, int seqNum){
        if(connectionChannelMap.containsKey(connId)) {
            int expectedSequence=expectedSequenceList.get(connId);
            if(expectedSequence==seqNum) {
                this.pendingEvents.add(new ProxyEvents(new byte[0], connId, ProxyEvents.ENDING, SelectionKey.OP_CONNECT, -1));
            }
            else{
                ConcurrentSkipListMap<Integer,byte[]> dataMap= connectionDataList.get(connId);
                dataMap.put(connId,null);
            }
        }
    }
    private void completeConnection(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        //Complete connecting. This would return true if the connection is successful
        try {
            socketChannel.finishConnect();
        } catch (IOException e) {
            e.printStackTrace();
            key.cancel();
            return;
        }

        //Since connection is established, show interest in writing data to the server


        key.interestOps(SelectionKey.OP_WRITE); //not sure all these op changes should be here
    }

    private void write(SelectionKey key) throws IOException{
        SocketChannel sockCh = (SocketChannel) key.channel();
        if(key.attachment()!=null) {
            int connId = (int) key.attachment();
            if (connectionDataList.containsKey(connId)) {
                //For now, assuming that we're reaching here only if the SYN packet is available
                ConcurrentSkipListMap<Integer,byte[]> dataMap = connectionDataList.get(connId);
                int expectedSequence=expectedSequenceList.get(connId);
                NavigableSet<Integer> seqNumberList=dataMap.keySet();
                for(int availSequence:seqNumberList) {
                    if (availSequence < expectedSequence) {
                        ByteBuffer buf = ByteBuffer.wrap(dataMap.get(availSequence));
                        int x = sockCh.write(buf);
                        if (buf.remaining() > 0) {
                            break;
                        }
                    }
                }
                    for(int availSequence:seqNumberList){
                        if(availSequence<expectedSequence) {
                            dataMap.remove(availSequence);
                        }
                    }
                    if((dataMap.containsKey(expectedSequence))&&(dataMap.get(expectedSequence)==null)){
                        this.pendingEvents.add(new ProxyEvents(new byte[0], connId, ProxyEvents.ENDING, SelectionKey.OP_CONNECT, -1));
                    }
                    else {
                        key.interestOps(SelectionKey.OP_READ);
                    }
            }
            else{
                key.interestOps(SelectionKey.OP_WRITE);//seriously this looks so wrong to me
            }
        }
        //This case is when the LP socket is ready to be written into
        else{
            for(Map.Entry<Integer,ArrayList<byte[]>> connections:responseDataList.entrySet()) {
                ArrayList<byte[]> dataList = responseDataList.get(connections.getKey());

                while (!dataList.isEmpty()) {
                    ByteBuffer buf = ByteBuffer.wrap(dataList.get(0));
                    int x = sockCh.write(buf);
                    if (buf.remaining() > 0) {
                        break;
                    }
                    dataList.remove(0);
                }
                if (dataList.isEmpty()) {
                    key.interestOps(SelectionKey.OP_READ);

                    //done performing write, so add back to the freePipes
                    freePipes.add(sockCh);
                }
            }
        }
    }
}
