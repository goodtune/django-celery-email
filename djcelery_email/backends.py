from django.conf import settings
from django.core.mail.backends.base import BaseEmailBackend

from djcelery_email.tasks import send_emails
from djcelery_email.utils import chunked, email_to_dict


class CeleryEmailBackend(BaseEmailBackend):
    def __init__(self, fail_silently=False, **kwargs):
        super(CeleryEmailBackend, self).__init__(fail_silently)
        self.init_kwargs = kwargs

    def send_messages(self, email_messages):
        result_tasks = []
        messages = [email_to_dict(msg) for msg in email_messages]
        for chunk in chunked(messages, settings.CELERY_EMAIL_CHUNK_SIZE):
            result_tasks.append(send_emails.delay(chunk, self.init_kwargs))
        return result_tasks


class CompatibleCeleryEmailBackend(CeleryEmailBackend):
    """
    ``BaseEmailBackend.send_messages`` does not impose a return type so we have
    a conundrum. Should we use this to our advantage and return anything
    (because there is no spec disallowing it)?

    Alternatively we could behave like the bundled backends such as
    ``django.core.mail.backends.smtp.EmailBackend``.

    This implementation attempts to act like the latter.

    Underneath we still use the ``CeleryEmailBackend`` but return the count.
    """
    def send_messages(self, email_messages):
        if not email_messages:
            return
        async_results = (
            super(CompatibleCeleryEmailBackend, self)
            .send_messages(email_messages)
        )
        return len(async_results)
