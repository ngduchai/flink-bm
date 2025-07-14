hostname=$1
curl --noproxy $hostname -H "Accept: application/json" http://$hostname:8081/jobs/overview | jq .

