#GossAgent

Goss Agent is the agent part of the Goss(Game Operation Support System), which enables people manage their game servers easier.

#Features

+ start/stop game apps
+ update game apps(multi apps which distributed on multi physical servers update is supported)
+ update game scripts(multi scripts which distributed on multi physical servers update is supported)
+ game server console view support
+ view multi game server consoles is supported, which is called console wall
+ configurations switch
+ database backup
+ agents status view support
+ the agent support self update and restart

#Get started

You should have [virtualenv](http://www.virtualenv.org "virtualenv") and [virtualenvwrapper](http://virtualenvwrapper.readthedocs.org/en/latest/index.html "virtualenvwrapper") installed on your system before you start.

    $ git clone https://github.com/jhezjkp/mfGossAgent.git
    $ cd mfGossAgent
    $ mkvirtualenv mfGossAgent
    $ pip install -r requirements.txt
    $ sh start.sh
