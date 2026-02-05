# **Possible errors and solutions**


## **Prolem #1: Database connection failed**
```bash
$ python3 src/run.py -c config.ini
[!] Error: Database connection failed.
connection to server at "localhost" (127.0.0.1), port 5432 failed: Connection refused
        Is the server running on that host and accepting TCP/IP connections?
```
### **Solution**

The service is down. Run the following command:

```bash
$ sudo service postgresql start
```
Or
```bash
$ sudo systemctl start postgresql
```
\* If your user is already has admin privileges (this means, you are logged as root), so `sudo` is not needed.