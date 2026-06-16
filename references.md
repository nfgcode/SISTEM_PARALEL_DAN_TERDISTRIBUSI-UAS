# Panduan Referensi Ilmiah & Sourcing Data
**UAS Sistem Paralel dan Terdistribusi**

Dokumen ini mencantumkan detail pustaka, jurnal ilmiah, dan buku acuan yang mendasari keputusan desain arsitektur **Pub-Sub Log Aggregator Terdistribusi** dalam proyek tugas UAS ini.

---

## 1. Buku Acuan Utama (Main Textbook)

Sistem ini didesain dengan merujuk langsung pada prinsip dasar sistem terdistribusi yang dijabarkan dalam:

* **Sitasi APA 7th**: 
  Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2012). *Distributed Systems: Concepts and Design* (5th ed.). Addison-Wesley.
* **Topik yang Diadopsi**:
  * **Bab 1 & 2**: Karakteristik (ketiadaan clock global, kegagalan independen) dan pola arsitektur Pub-Sub serta microservices.
  * **Bab 3 & 4**: Model komunikasi multicast, protokol request-reply asinkron, dan skema pengenal unik global (UUID).
  * **Bab 5**: Pengurutan kejadian (*logical order*) menggunakan timestamp fisik terkoordinasi dan monotonic counter.
  * **Bab 6**: Kegagalan saluran komunikasi, pendeteksian crash, dan ketahanan data.
  * **Bab 7**: Konsistensi akhir (*eventual consistency*) pada sistem penyimpanan replikasi.
  * **Bab 8 & 9**: Transaksi ACID di database relasional, tingkat isolasi transaksi (*isolation level*), pencegahan lost-update, serta kontrol konkurensi berbasis kunci baris (*row-locking*).
  * **Bab 10 - 13**: Keamanan jaringan lokal dan koordinasi antar service terdistribusi.

---

## 2. Jurnal & Makalah Ilmiah Pendukung (Scientific Papers & Journals)

Untuk memberikan landasan akademik yang lebih mendalam mengenai pola perancangan *Idempotent Consumer* dan *Log Aggregator*, kami menggunakan referensi jurnal ilmiah berikut:

### A. Perancangan Log Aggregator Terdistribusi
* **Referensi**: 
  Kreps, J., Narkhede, N., & Rao, J. (2011). Kafka: a Distributed Messaging System for Log Processing. *Proceedings of the NetDB*, 1-7.
* **Relevansi**: Makalah ini menjelaskan kegunaan message broker perantara untuk mengumpulkan log dalam skala besar secara asinkron (menggunakan antrean pesan seperti yang diimplementasikan melalui Redis). Makalah ini juga menyoroti mengapa model antrean FIFO (First-In, First-Out) sangat cocok untuk pemrosesan log telemetri terdistribusi.

### B. Pola Komunikasi At-Least-Once & Idempotent Consumer
* **Referensi**:
  Decandia, G., Hastorun, D., Jampani, M., Kakulapati, G., Lakshman, A., Pilchin, A., Sivasubramanian, S., Vosshall, P., & Vogels, W. (2007). Dynamo: Amazon’s Highly Available Key-value Store. *ACM SIGOPS Operating Systems Review*, 41(6), 205-220.
* **Relevansi**: Membahas konsep *eventual consistency* dan bagaimana sistem terdistribusi harus dirancang untuk menerima pengiriman data duplikat akibat kegagalan jaringan (*at-least-once delivery*). Menekankan pentingnya penanganan idempotensi di sisi penerima (consumer) untuk memastikan keakuratan data akhir.

### C. Kontrol Konkurensi Tanpa Kunci (Lock-free Concurrency)
* **Referensi**:
  Stonebraker, M., Madden, S., & Abadi, D. J. (2007). The End of an Architectural Era (It's Time for a Complete Rewrite). *Proceedings of the 33rd International Conference on Very Large Data Bases (VLDB)*, 1150-1160.
* **Relevansi**: Menganalisis trade-off performa antara penguncian pesimis (*pessimistic locking*) tradisional dengan pendekatan optimis berbasis constraint unik database (seperti indeks unik gabungan dan upsert `ON CONFLICT` pada PostgreSQL) yang kami gunakan untuk deduplikasi atomik dalam consumer worker.

---

## 3. Panduan Pemetaan Bab Teori & Implementasi Praktis

Untuk memudahkan pemahaman alur desain, saya memetakan teori di buku acuan ke dalam file implementasi kode program:

| Soal Teori | Bab Acuan | File Implementasi Terkait | Keterangan Praktis |
| :--- | :--- | :--- | :--- |
| **T1 & T2** | Bab 1 & 2 | [docker-compose.yml](file:///D:/sister-uas/docker-compose.yml) | Memisahkan komponen API, Broker Redis, dan Consumer ke dalam container terpisah untuk mencapai *loose coupling*. |
| **T3 & T4** | Bab 3 & 4 | [endpoints.py](file:///D:/sister-uas/aggregator/app/api/endpoints.py) & [consumer.py](file:///D:/sister-uas/aggregator/app/worker/consumer.py) | Skema validasi Pydantic untuk memverifikasi payload event, generator data di simulator, dan ID berbasis UUID. |
| **T5** | Bab 5 | [publisher.py](file:///D:/sister-uas/publisher/publisher.py) | Simulator menyertakan urutan timestamp ISO 8601 di setiap event sebelum dikirim secara paralel. |
| **T6** | Bab 6 | [consumer.py](file:///D:/sister-uas/aggregator/app/worker/consumer.py) | Blok kode `process_with_retry` menerapkan penanganan pengecualian database (*exception handling*) dengan jeda eksponensial (*exponential backoff*). |
| **T7** | Bab 7 | [database.py](file:///D:/sister-uas/aggregator/app/core/database.py) | Kueri `INSERT ... ON CONFLICT DO NOTHING` mengabaikan duplikat dan memperbarui metrik secara eventual consistent. |
| **T8 & T9** | Bab 8 & 9 | [database.py](file:///D:/sister-uas/aggregator/app/core/database.py) | Blok transaksi `async with conn.transaction(isolation='read_committed')` menjaga pembaruan counter stats terhindar dari lost-update. |
| **T10** | Bab 10-13 | [docker-compose.yml](file:///D:/sister-uas/docker-compose.yml) | Penyetelan jaringan internal kontainer (`networks`), volume persisten (`volumes`), dan readiness check (`healthcheck`). |
