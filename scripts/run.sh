java -Xms4096M -Xmx4096M -Xss10M \
    -jar io-opentargets-platform-ddr-assembly-0.1.0.jar -i "$1" -o "$2" --kwargs log-level=INFO &> salida.log
