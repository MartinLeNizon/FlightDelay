UPDATE flights
SET scheduled_time = TO_CHAR(scheduled_time::timestamp, 'DDHH24MISS') || 'Z';
