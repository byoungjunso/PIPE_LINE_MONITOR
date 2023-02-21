# PIPE_LINE_MONITOR

- Pipe line Monitor는 일 단위 35TB / 380억 record 가 수집되는 플랫폼에서 데이터 수집, 분석 감시를 위한 환경을 제공 합니다.

- DataSource
    - Application Log: 로그 상에 Application이 수집한 파일의 size, record 건수를 실시간으로 수집하여 추이를 감시 합니다.
    - System Stats/App Resource Stats: CPU, Memory, Disk, Network, Inode, FileSystem 등의 실시간 현황 및 추이를 제공하여 트러블 슈팅을 지원 합니다.
    - Kafka Burrow: offset의 변화량을 통해 데이터 전송량을 감시 합니다.
    - Spark Streaming: API에서 제공하는 Micro Batch 통계를 수집하여 각 Job 별로 소요 시간을 감시 합니다.
- 구조도

![pipeline_arch drawio (2)](https://user-images.githubusercontent.com/86950682/219941664-28fc0a2b-e6e6-4e54-9431-468c66b7b7a9.png)
