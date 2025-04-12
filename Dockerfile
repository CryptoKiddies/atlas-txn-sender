FROM ubuntu:22.04

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    pkg-config \
    libssl-dev \
    libudev-dev \
    zlib1g-dev \
    llvm \
    clang \
    cmake \
    make \
    libprotobuf-dev \
    protobuf-compiler \
    git \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Install Ansible
RUN pip3 install ansible

# Install Datadog role
RUN ansible-galaxy install datadog.datadog

# Create app directory
WORKDIR /home/ubuntu/atlas-txn-sender

# Copy the application files
COPY . .

# Run ansible playbook
RUN ansible-playbook -i ansible/inventory/hosts.yml ansible/deploy_atlas_txn_sender.yml

# Expose the port the app runs on
EXPOSE 4040

# Start the application (using the path where ansible built it)
CMD ["/home/ubuntu/atlas-txn-sender/target/release/atlas_txn_sender"] 