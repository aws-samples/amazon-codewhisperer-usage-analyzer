#!/usr/bin/env python3
import os

import aws_cdk as cdk

from pipeline.code_whisperer_professional_edition_analysis_stack import CodeWhispererProfessionalEditionAnalysisStack
from cdk_nag import AwsSolutionsChecks

app = cdk.App()
CodeWhispererProfessionalEditionAnalysisStack(app, "CodeWhispererProfessionalEditionAnalysisStack")

cdk.Aspects.of(app).add(AwsSolutionsChecks(verbose=True))
app.synth()