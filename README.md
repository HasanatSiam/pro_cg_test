Git Repo:
	git clone https://github.com/HasanatSiam/pro_cg_test
	

Installation Command:
	pip install git+https://github.com/sibson/redbeat.git

PostgreSQL:
Local:
	sudo systemctl start postgresql@16-main
Tunneling:
	ssh db-tunnel-infodetics

/Virtual environment
    virtualenv .procg_venv
    .procg_venv\scripts\activate

/Installation
    pip install -r requirment.txt
/Run flask application
    flask run

/Run celery
    celery -A executors worker -E --loglevel=info --pool=gevent

/Run flower
    celery -A executors flower

/Run redbeatscheduler
    celery -A api.tasks beat --scheduler redbeat.RedBeatScheduler -l info



