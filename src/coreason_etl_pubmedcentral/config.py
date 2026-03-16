# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import dlt
from dlt.common.configuration import configspec, with_config
from dlt.common.configuration.specs import BaseConfiguration


@configspec
class PubMedCentralConfiguration(BaseConfiguration):
    __section__ = "pubmed_central"
    """
    AGENT INSTRUCTION: Strict configuration class defining the core execution
    parameters for the PubMed Central ETL pipeline.
    """

    s3_bucket: str = "pmc-oa-opendata"
    ftp_host: str = "ftp.ncbi.nlm.nih.gov"
    ftp_path: str = "/pub/pmc/"
    manifest_commercial: str = "oa_comm/oa_comm.filelist.csv"
    manifest_noncommercial: str = "oa_noncomm/oa_noncomm.filelist.csv"


@with_config(spec=PubMedCentralConfiguration, sections=("pubmed_central",))
def get_pipeline_config(config: PubMedCentralConfiguration = dlt.config.value) -> PubMedCentralConfiguration:
    """
    Retrieves the pipeline configuration using dlt's injection mechanism.
    """
    return config
