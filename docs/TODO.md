

Redo all the tests-live to use rust client and a live instance of murmur, not embedded code!!



Bechnmark the special sqlite experimental with begin concurrent



The clients needs to enforce one thing, all writes must be serialize by a single thread, and readers are sharing a connection pool that can read concurrently.
Sqlite can only have a single writter so this is important.  This might already be implemetned in the client if it is let me know.

Rust should use Axum, Tokio and Rusqlite.  

Python use FastAPI, asyncio and sqlite3. 

In Go is a fresh unbuffered channel containing a result struct created per-request.




must support /install and /uninstall on windows or linux as a service.
    if does not exist create a murmur user  
    ask for a path for installation
    ask for a path for data
    if the service is already running ask to stop it 
    copy the file on destination
    restart of service if it was running or ask the user if the service should be started.
    ** check ../OVERWATCH21 /install and copy the same actions here actually what's missing from the list.
    


Continuous Improvement:

    Improve speed: Scalability improvement SCALABILITY.md

    Improve stability: 
        Run all unit test in ./tests
        Run all the ./tests-live 
        If error occur find the root cause and fix the error than resume testing.
        
        
