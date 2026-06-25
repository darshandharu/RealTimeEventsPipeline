"""Dead Letter Queue package for the Real-Time Events Analytics Pipeline.

Exposes the producer-side DLQ writer and the canonical DLQ record builder used
by both the producer and the Spark streaming job.
"""

from dlq.dead_letter_queue import DeadLetterQueue, build_dlq_record

__all__ = ["DeadLetterQueue", "build_dlq_record"]
