from backlog.models import *


def clean_db():
    AssemblyJob.objects.all().delete()
    AnnotationJob.objects.all().delete()
    AssemblyJobStatus.objects.all().delete()

    RunAssembly.objects.all().delete()
    RunAssemblyJob.objects.all().delete()
    Run.objects.all().delete()
    Assembly.objects.all().delete()
    Study.objects.all().delete()

    UserRequest.objects.all().delete()
    User.objects.all().delete()

    Pipeline.objects.all().delete()
    Assembler.objects.all().delete()
