package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// userData sets up the guest on its first boot: it installs the packages
// from the disk labelled SBXDEBS, runs the guest program at every boot if
// a disk labelled SBXRUN brings one, and turns off what would import
// pools, wait for a network or otherwise run beside the test.
const userData = `#cloud-config
write_files:
  - path: /etc/systemd/system/sbx.service
    content: |
      [Unit]
      Description=swarmblk crash test
      After=local-fs.target
      [Service]
      Type=simple
      ExecStart=/usr/local/sbin/sbx-start
      StandardOutput=journal+console
      [Install]
      WantedBy=multi-user.target
  - path: /usr/local/sbin/sbx-start
    permissions: '0755'
    content: |
      #!/bin/sh
      # Runs the crash test's guest program and command from the disk
      # labelled SBXRUN, if the VM has one.
      udevadm settle -t 30
      dev=$(blkid -L SBXRUN) || exit 0
      mkdir -p /mnt/run && mount -o ro "$dev" /mnt/run || exit 1
      cp /mnt/run/crashtest /usr/local/sbin/crashtest && chmod 755 /usr/local/sbin/crashtest || exit 1
      cmd=$(cat /mnt/run/cmd)
      umount /mnt/run
      exec /usr/local/sbin/crashtest guest $cmd
runcmd:
  - [sh, -c, 'mkdir -p /mnt/debs && mount -o ro /dev/disk/by-label/SBXDEBS /mnt/debs && dpkg -i /mnt/debs/*.deb; umount /mnt/debs']
  - [systemctl, enable, sbx.service]
  - [systemctl, mask, zfs-import-cache.service, zfs-import-scan.service, zfs-mount.service, zfs-share.service, zfs-zed.service, zfs-volume-wait.service, multipathd.service, multipathd.socket, snapd.service, snapd.socket, snapd.seeded.service, unattended-upgrades.service, apt-daily.timer, apt-daily-upgrade.timer, motd-news.timer, man-db.timer, fstrim.timer, e2scrub_all.timer, e2scrub_reap.service, fwupd-refresh.timer, systemd-networkd-wait-online.service]
  - [touch, /etc/cloud/cloud-init.disabled]
  - [sh, -c, 'echo "SBX TOOLS $(uname -r); $(zfs version 2>&1 | tr "\n" " "); $(mkfs.xfs -V); $(btrfs --version | head -1); $(e2fsck -V 2>&1 | head -1)" > /dev/ttyS0']
  - [sh, -c, 'modprobe zfs && echo "SBX PREP OK" > /dev/ttyS0']
power_state:
  mode: poweroff
  condition: true
`

// cmdImage builds the guest image, guest.qcow2 in the work directory.
func cmdImage(args []string) error {
	fs := flag.NewFlagSet("image", flag.ExitOnError)
	work := fs.String("work", defaultWork(), "work `directory`")
	fs.Parse(args)
	prep := filepath.Join(*work, "prep")
	if err := os.MkdirAll(prep, 0o700); err != nil {
		return err
	}
	for name, content := range map[string]string{"user-data": userData, "meta-data": "instance-id: sbx-prep\nlocal-hostname: sbx\n"} {
		if err := os.WriteFile(filepath.Join(prep, name), []byte(content), 0o600); err != nil {
			return err
		}
	}
	img := filepath.Join(*work, "guest.qcow2")
	tmp := img + ".new"
	for _, c := range [][]string{
		{"cloud-localds", filepath.Join(prep, "seed.img"), filepath.Join(prep, "user-data"), filepath.Join(prep, "meta-data")},
		{"genisoimage", "-quiet", "-o", filepath.Join(prep, "debs.iso"), "-V", "SBXDEBS", "-J", "-R", filepath.Join(*work, "debs")},
		{"qemu-img", "convert", "-O", "qcow2", filepath.Join(*work, "noble-server-cloudimg-amd64.img"), tmp},
	} {
		if err := run(c[0], c[1:]...); err != nil {
			return err
		}
	}
	console := filepath.Join(prep, "console")
	cf, err := os.Create(console)
	if err != nil {
		return err
	}
	defer cf.Close()
	cmd := exec.Command("qemu-system-x86_64", "-enable-kvm", "-cpu", "host", "-m", "2048", "-smp", "2",
		"-nographic", "-nic", "none",
		"-drive", "file="+tmp+",if=virtio,format=qcow2",
		"-drive", "file="+filepath.Join(prep, "seed.img")+",if=virtio,format=raw,readonly=on",
		"-drive", "file="+filepath.Join(prep, "debs.iso")+",if=virtio,format=raw,readonly=on")
	cmd.Stdout, cmd.Stderr = cf, cf
	start := time.Now()
	if err := cmd.Start(); err != nil {
		return err
	}
	timer := time.AfterFunc(30*time.Minute, func() { cmd.Process.Kill() })
	err = cmd.Wait()
	timer.Stop()
	b, _ := os.ReadFile(console)
	var report []string
	for _, m := range sbxLine.FindAllStringSubmatch(string(b), -1) {
		report = append(report, m[0])
	}
	fmt.Printf("guest image: first boot took %v\n%s\n", time.Since(start).Round(time.Second), strings.Join(report, "\n"))
	if err != nil || !strings.Contains(string(b), "SBX PREP OK") {
		return fmt.Errorf("the guest's first boot failed (%v); see %s", err, console)
	}
	return os.Rename(tmp, img)
}
