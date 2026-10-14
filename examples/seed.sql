-- Our side of 2026-10-01: transfers sent to the "bank" provider.
INSERT INTO transfers (provider, reference, amount_minor, currency, status, created_at) VALUES
    ('bank', 'TRF-1001', 250000, 'USD', 'pending',   '2026-10-01 09:12:00+00'),
    ('bank', 'TRF-1002', 120000, 'USD', 'pending',   '2026-10-01 10:40:00+00'),
    ('bank', 'TRF-1003',  99900, 'USD', 'succeeded', '2026-10-01 11:05:00+00'),
    ('bank', 'TRF-1004', 500000, 'USD', 'pending',   '2026-10-01 13:30:00+00'),
    ('bank', 'TRF-1005',  75000, 'USD', 'succeeded', '2026-10-01 15:00:00+00'),
    ('bank', 'TRF-1006',  30000, 'USD', 'pending',   '2026-10-01 16:45:00+00'),
    ('bank', 'TRF-0990',  42000, 'USD', 'pending',   '2026-09-29 18:00:00+00')
ON CONFLICT (reference) DO NOTHING;
