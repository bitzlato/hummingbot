# 1) Clone Hummingbot repo
git clone https://github.com/bitzlato/hummingbot.git
# 2) Navigate into the hummingbot folder
cd hummingbot
# 3) switch branch 
git checkout bitzlato_stable
# 4) Run install script
./install
# 5) Activate environment
conda activate hummingbot
# 6) Compile
./compile
# 7) Run Hummingbot
bin/hummingbot.py
