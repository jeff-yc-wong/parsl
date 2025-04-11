FROM python:3.12.10-slim-bookworm AS base

# Update repositories
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openssh-server git make gcc g++ vim sudo && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create a user matching the builder's username
ARG USERNAME=parsl
ARG USER_UID

RUN useradd -u $USER_UID -ms /bin/bash $USERNAME && \
    echo "$USERNAME:redhat" | chpasswd && \
    adduser $USERNAME sudo && \
    echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

# Configure SSH server
# Set no password for user login
RUN sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
EXPOSE 22

COPY ./Makefile /usr/local/bin
COPY ./cpu-benchmark.cpp /usr/local/bin
RUN cd /usr/local/bin && make && cd ~   

RUN python3 -m pip install --break-system-packages pandas pathos

# Switch to new user
USER $USERNAME
WORKDIR /home/$USERNAME

# SSH setup
RUN mkdir -p /home/$USERNAME/.ssh
COPY --chown=$USERNAME:$USERNAME ./id_rsa.pub /home/$USERNAME/.ssh/authorized_keys
RUN chmod 700 /home/$USERNAME/.ssh && \
chmod 600 /home/$USERNAME/.ssh/authorized_keys && \
chown -R $USERNAME:$USERNAME /home/$USERNAME/.ssh


COPY --chown=$USERNAME:$USERNAME ./entry.sh /bin
RUN chmod +x /bin/entry.sh

FROM base AS builder

ARG CACHEBUST=1

COPY ./wfbench /usr/local/bin

# Install parsl
RUN git clone --depth 1 --branch scheduling_using_simulation https://github.com/jeff-yc-wong/parsl.git && \
    sudo python3 -m pip install --break-system-packages parsl/ && \ 
    sudo rm -rf parsl/

# Install WfCommons
# RUN git clone --depth 1 --branch feature/wfbench_updates https://github.com/wfcommons/WfCommons.git
# RUN pip install WfCommons/

ENTRYPOINT ["/bin/entry.sh"]
