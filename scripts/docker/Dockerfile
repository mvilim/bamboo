FROM quay.io/xhochy/arrow_manylinux1_x86_64_base
COPY build_boost.diff /
RUN patch build_boost.sh build_boost.diff
RUN ./build_boost.sh
COPY install_proto.sh /
RUN ./install_proto.sh
