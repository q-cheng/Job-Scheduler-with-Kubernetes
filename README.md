# p32 Starter

The starter includes:
  * `src/` - skeleton implementations of the policy you need to implement
  * `bin/` 
    * `p32test` - program to test your policy (run `bin/p32test -h` for more details)
    * `p32submit` - program to submit your policy (run `bin/p32submit -h` for more details)
    * `job-mon` and `job-plot` - export jobs status into CSV and plot it graphically into a PDF (notice that you genereally don't need to run them manually - `p32test` and `p32submit` will do everything for you).
  * `traces/` - sample traces. Each line of each trace comprises timestamp (in seconds, relative to the time the trace started to run), job type (see table in the writeup in TPZ), number of tasks (nodes), and the value 0.

## Pulling starter updates
1. Add the student common starter code repository as a remote (needs to be done only once):
    ```
    $ git remote add starter git@github.com:cmu15719/p3.2-starter.git
    ```
1. Check if there are pending changes:
    ```
    $ git fetch starter
    $ git log master..starter/master
    ```
    If the output is not empty - there are pending changes you need to pull.
1. Pull from the student common starter code repository:
    ```
    $ git pull starter master
    ```
1. Resolve potential conflicts by merging
