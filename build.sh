cd build
rm -rf ./*
cmake -DCMAKE_BUILD_TYPE=Debug ..
# cmake ..
make -j$(grep -c processor /proc/cpuinfo)

