# Setting up chameleon instances

## Creating a lease
1. Inside the TACC Chameleon Portal, go to Leases, then click on the host calendar, to find avaialble hardware resources on TACC based on node_types.
2. Once you found an available node, you can click on the node name to reserve it, the maximum length you can reserve a node for is 7 days (in which you can renew it once it's about to end).
3. There is no need to reserve a network, but make sure to reserve 1 floating IP for the lease. You will need it for creating the compute instance.
4. Create the lease, and reload page till you see the lease's status become ACTIVE. You can then begin the next step.

## Creating an instance
1. Inside the TACC Chameleon Portal, go to Compute->Instances, click "Lauch Instance".
2. Make sure you select the lease reservation you just created above.
3. For selecting the source, you can use "CC-Ubuntu24.04", and for networks, select sharednet1.
4. Once the instance has spawned and is running (this might take around 10 minutes), go into Network->Floating IP, then associate the floating IP to the instance you just launched. This will be the IP you use to SSH into the instance.
5. To set up SSH keys, go to Compute->Instance, then click on the instance you just created and you can go into the Console tab to open up a Terminal within the instance to set up authorized_keys.
6. Make sure dirt is able to SSH into the instance, as that is how we will be connecting the Parsl workers.
7. Once the instance has spawned and is active, go into Network->Floating IP, then associate the floating IP to the instance you just launched. This will be the IP you use to SSH into the instance.
8. To set up SSH keys, go to Compute->Instance, then click on the instance you just created and you can go into the Console tab to open up a Terminal within the instance to set up authorized_keys.
9. Make sure dirt is able to SSH into the instance, as that is how we will be connecting the Parsl workers.
10. Install Docker Engine inside the instance follwing the documentation [here](https://docs.docker.com/engine/install/ubuntu/)
11. Mount the shared drive using the following command:

    ```bash
    mkdir /home/cc/sus_env && sudo mount <dirt02's public ip>:/home/wongy/sus_env /home/cc/sus_env
    ```

12. Build the docker image 

    ```bash 
    cd /home/cc/sus_env/parsl/docker && bash build.sh
    ```

## ✅ All Done!

That's it! Everything should be done. You can repeat these steps for each cluster you would like to have.

