FROM quay.io/pypa/manylinux2014_x86_64
COPY install_proto.sh /
RUN ./install_proto.sh
RUN yum install -y boost-devel
RUN yum install -y zlib-devel
