CREATE TABLE public.readings (id integer, device text, metric text, value double precision, ts timestamp);
INSERT INTO public.readings (id, device, metric, value, ts) VALUES
(1,'pump-3','temperature',70.1,'2026-06-01 08:02:00'),
(2,'pump-3','temperature',70.5,'2026-06-01 08:07:00'),
(3,'pump-3','temperature',70.3,'2026-06-01 08:14:00'),
(4,'pump-3','temperature',70.4,'2026-06-01 09:01:00'),
(5,'pump-3','temperature',95.0,'2026-06-01 09:08:00'),
(6,'pump-3','vibration',0.20,'2026-06-01 08:03:00'),
(7,'pump-3','vibration',0.22,'2026-06-01 08:09:00'),
(8,'pump-3','vibration',0.19,'2026-06-01 09:02:00'),
(9,'pump-3','vibration',0.21,'2026-06-01 09:10:00'),
(10,'pump-7','temperature',68.0,'2026-06-01 08:05:00'),
(11,'pump-7','temperature',68.2,'2026-06-01 08:11:00'),
(12,'pump-7','temperature',67.9,'2026-06-01 09:04:00'),
(13,'pump-7','temperature',68.1,'2026-06-01 09:12:00'),
(14,'pump-7','vibration',0.30,'2026-06-01 08:06:00'),
(15,'pump-7','vibration',0.31,'2026-06-01 08:13:00'),
(16,'pump-7','vibration',0.29,'2026-06-01 09:05:00');