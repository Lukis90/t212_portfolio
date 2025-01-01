# T212 porfolio pnl tracker

On your first launch, you might need to try to run `python portfolio-tracker.py` several time to download historical exchange rates.

* To start run `pip install -r requirements.txt` (don't forget new python environment).
* Download historical data from t212 with orders and add it to `data` folder inside porfolio folder.
* Create .env file inside portfolio folder and add for which year you want to calculate pnl. For example.: `WHICH_YEAR = 2023` will calculate it for year 2023. There already is .env.sample file, you can remove `.sample` part and have a working project.
* Run `python portfolio-tracker` inside portfolio folder.
