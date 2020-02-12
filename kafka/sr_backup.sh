kafka-console-consumer --bootstrap-server localhost:9092 --topic _schemas --from-beginning --property print.key=true --timeout-ms 10000 1>  ~/sr_backup/schemas.log
curl -s -X GET http://localhost:8081/subjects | tr ',' "\n" > ~/sr_backup/schemas_list.log
sed -i "1s/^/********** $(date) **********\n\n/" ~/sr_backup/schemas_list.log
cd ~/sr_backup/ && git add schemas.log schemas_list.log && git commit -m 'sr_backup' &&  git push
