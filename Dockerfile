FROM gcr.io/dataflow-templates-base/python38-template-launcher-base:20220418_RC00

ARG WORKDIR=/dataflow/template
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

COPY modules ./modules
COPY schema ./schema
COPY ./*.py ./
COPY ./*.sh ./
COPY ./requirements.txt ./

# combined or all
ARG TYPE

# ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/run_${TYPE}.py"

RUN pip install -U -r ./requirements.txt
