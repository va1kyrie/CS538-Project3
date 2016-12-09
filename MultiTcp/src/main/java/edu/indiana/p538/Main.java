package edu.indiana.p538;

public class Main {

    public static void main(String[] args) {
        //number of pipes
        int pipes = Integer.parseInt(args[0]);

        try{
            ProxyWorker worker = new ProxyWorker();
            new Thread(worker).start();
            new Thread(new Proxy(AppConstants.PROXY_PORT, worker,pipes)).start();
        }catch(Exception e){
            System.err.println("Error opening socket");
            System.exit(-1);
        }

    }
}
