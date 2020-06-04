#!/bin/bash

usage()
{
    echo "Usage: $0 -c <custom-type> -i <iso-path> -o <output-path> -s <y or n>"
}

check_tool_or_die()
{
    local tool=$1
    ! which $tool &>/dev/null && echo "ERROR: no $tool installed" && exit -1
}

check_tools_or_die()
{
    check_tool_or_die rpmbuild
    check_tool_or_die createrepo
    check_tool_or_die genisoimage
    check_tool_or_die isohybrid
    check_tool_or_die implantisomd5
    check_tool_or_die checkisomd5    
}

parse_options()
{
    while getopts "c:i:o:s:" arg; do
        case $arg in
            c)
                custom_type=$OPTARG
                ;;
            i)
                iso_path=$OPTARG
                ;;
            o)
                output_path=$OPTARG
                ;;
            s)
                add_hg=$OPTARG
                ;;
        esac
    done
}

parse_options "$@"

if [ ! -f "$iso_path" ]; then
    usage
    exit -1
fi

if [ "$output_path" == "" ]; then
    usage
    exit -1
fi

if [ "$custom_type" == "" ]; then
    echo "### prepare to build basic version image ..."
else
    echo "### prepare to build $custom_type version image ..."
fi
sleep 2

check_tools_or_die

ks_path=configs/ks.cfg
isolinux_path=configs/isolinux.cfg
efigrub_path=configs/grub.cfg

# prepare build environment
TEMP_DIR=tmp/isodir.tmp
RAW_ISO_DIR=tmp/isodir.mnt
sudo umount -l $RAW_ISO_DIR/Packages
sudo umount -l $RAW_ISO_DIR
rm -rf $TEMP_DIR
rm -rf $RAW_ISO_DIR
mkdir -p $RAW_ISO_DIR
if ! sudo mount $iso_path $RAW_ISO_DIR; then
    echo "Failed to mount $iso_path to $RAW_ISO_DIR"
    exit -1
fi
sudo mount -t ramfs ramfs $RAW_ISO_DIR/Packages
cp -rfv $RAW_ISO_DIR $TEMP_DIR
sudo umount $RAW_ISO_DIR/Packages
for file in `cat packages.list`; do
    cp -f $RAW_ISO_DIR/Packages/$file.rpm $TEMP_DIR/Packages
done

# prepare configs and packages
cp -f $ks_path $TEMP_DIR
cat  $ks_path | sed '/END/d' | sed '/part/d' | sed '/ignoredisk/d' | sed '/bootloader/d' >> $TEMP_DIR/ks_m.cfg
cp -f $isolinux_path $TEMP_DIR/isolinux
cp -f $efigrub_path $TEMP_DIR/EFI/BOOT/

# Cancel automatic partition of system disk
# FIXME: bad style
sed -i '176,175d' "$TEMP_DIR/ks_m.cfg"

# haiguang iso-hg
if [ "$add_hg" == "y" ]; then
    HG_DIR=iso-hg
    cp -p $HG_DIR/vmlinuz $HG_DIR/initrd.img $TEMP_DIR/isolinux
    cp -p $HG_DIR/vmlinuz $HG_DIR/initrd.img $TEMP_DIR/images/pxeboot/
    cp -p $HG_DIR/kernel-3.10.0-862.el7.x86_64.rpm $TEMP_DIR/Packages
fi

cp extras/*.rpm $TEMP_DIR/Packages

# custom version
if [ "$custom_type" != "" ]; then
    mkdir -p $TEMP_DIR/custom-data
    cp -rf custom-data/$custom_type/* $TEMP_DIR/custom-data
fi

# generate iso
pushd $TEMP_DIR
createrepo -g repodata/*-comps.xml .
popd
genisoimage -v -cache-inodes -joliet-long -R -J -T -V CENTOS7-CW -o $output_path \
    -c isolinux/boot.cat -bisolinux/isolinux.bin -no-emul-boot -boot-load-size 4 \
    -boot-info-table -eltorito-alt-boot -e images/efiboot.img -no-emul-boot $TEMP_DIR

# make image disk bootable
isohybrid -u $output_path

# generate sha-256 checksum
implantisomd5 $output_path
sync
sudo bash -c "echo 1 >/proc/sys/vm/drop_caches"
checkisomd5 $output_path

md5sum $output_path >$output_path.md5sum.txt
md5sum -c $output_path.md5sum.txt
