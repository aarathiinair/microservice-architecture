#!/usr/bin/env python3
"""
Setup Script for Jira Attachment System

This script helps you set up your folder structure and organize files
for proper attachment handling.

Usage:
    python setup_attachments.py
"""

import os
from pathlib import Path
import shutil


def create_folder_structure():
    """Create the required folder structure"""
    print("\n" + "="*70)
    print("📁 CREATING FOLDER STRUCTURE")
    print("="*70)
    
    folders = [
        "original emails",
        "individual emails",
        "logs"
    ]
    
    for folder in folders:
        folder_path = Path(folder)
        if folder_path.exists():
            print(f"✅ '{folder}/' - Already exists")
        else:
            folder_path.mkdir(parents=True, exist_ok=True)
            print(f"✅ '{folder}/' - Created")
    
    print("\n✅ Folder structure ready!")


def check_msg_files():
    """Check for .msg files in the current directory and subdirectories"""
    print("\n" + "="*70)
    print("🔍 SEARCHING FOR .MSG FILES")
    print("="*70)
    
    msg_files = list(Path(".").rglob("*.msg"))
    
    if not msg_files:
        print("\n⚠️  No .msg files found in current directory or subdirectories")
        print("\n💡 Next steps:")
        print("   1. Download your .msg email files from Outlook")
        print("   2. Place them in the 'original_emails/' folder")
        print("   3. Ensure filenames contain machine names (e.g., DESDN01057)")
        return []
    
    print(f"\n✅ Found {len(msg_files)} .msg files:")
    
    files_by_folder = {}
    for msg_file in sorted(msg_files):
        folder = str(msg_file.parent)
        if folder not in files_by_folder:
            files_by_folder[folder] = []
        files_by_folder[folder].append(msg_file)
    
    for folder, files in files_by_folder.items():
        print(f"\n   {folder}/")
        for file in files[:3]:  # Show first 3
            size_kb = file.stat().st_size / 1024
            print(f"      - {file.name} ({size_kb:.1f} KB)")
        if len(files) > 3:
            print(f"      ... and {len(files) - 3} more files")
    
    return msg_files


def organize_files(msg_files):
    """Offer to move .msg files to original emails folder"""
    if not msg_files:
        return
    
    # Check if files are already in original emails
    files_to_move = [f for f in msg_files if "original emails" not in str(f.parent) and "original_emails" not in str(f.parent)]
    
    if not files_to_move:
        print("\n✅ All .msg files are already in 'original emails/' folder")
        return
    
    print("\n" + "="*70)
    print("📦 ORGANIZE FILES")
    print("="*70)
    print(f"\nFound {len(files_to_move)} .msg files outside 'original emails/' folder")
    print("\nWould you like to move them to 'original emails/'? (y/n): ", end="")
    
    response = input().strip().lower()
    
    if response == 'y':
        target_dir = Path("original emails")
        target_dir.mkdir(exist_ok=True)
        
        moved_count = 0
        for file in files_to_move:
            try:
                target_path = target_dir / file.name
                if target_path.exists():
                    print(f"   ⚠️  Skipping {file.name} (already exists in target)")
                else:
                    shutil.move(str(file), str(target_path))
                    print(f"   ✅ Moved: {file.name}")
                    moved_count += 1
            except Exception as e:
                print(f"   ❌ Error moving {file.name}: {e}")
        
        print(f"\n✅ Moved {moved_count} files to 'original emails/'")
    else:
        print("\n⏭️  Skipped moving files")


def verify_setup():
    """Verify the setup is correct"""
    print("\n" + "="*70)
    print("✅ VERIFICATION")
    print("="*70)
    
    checks = {
        "'original emails' folder exists": Path("original emails").exists(),
        "'individual emails' folder exists": Path("individual emails").exists(),
        ".msg files in 'original emails'": len(list(Path("original emails").glob("*.msg"))) > 0 if Path("original emails").exists() else False,
        ".json files in 'individual emails'": len(list(Path("individual emails").glob("*.json"))) > 0 if Path("individual emails").exists() else False,
        "processor.py exists": Path("processor.py").exists(),
        "main.py exists": Path("main.py").exists(),
        ".env file exists": Path(".env").exists(),
    }
    
    all_good = True
    for check, result in checks.items():
        status = "✅" if result else "⚠️ "
        print(f"   {status} {check}")
        if not result:
            all_good = False
    
    if all_good:
        print("\n🎉 Setup looks good! You're ready to run:")
        print("   python main.py --test")
    else:
        print("\n⚠️  Some items need attention. See above for details.")


def show_next_steps():
    """Show next steps"""
    print("\n" + "="*70)
    print("🚀 NEXT STEPS")
    print("="*70)
    
    print("\n1. Test your setup:")
    print("   python test_attachments.py --mode check")
    
    print("\n2. Create a test ticket:")
    print("   python test_attachments.py --mode create --file \"original emails/your-file.msg\"")
    
    print("\n3. Process your emails:")
    print("   python main.py --test")
    
    print("\n4. Process all emails:")
    print("   python main.py")
    
    print("\n📚 For troubleshooting, see: TROUBLESHOOTING_GUIDE.md")


def main():
    print("\n" + "="*70)
    print("🛠️  JIRA ATTACHMENT SYSTEM SETUP")
    print("="*70)
    print("\nThis script will help you set up the folder structure")
    print("and organize your files for proper attachment handling.")
    
    # Step 1: Create folders
    create_folder_structure()
    
    # Step 2: Find .msg files
    msg_files = check_msg_files()
    
    # Step 3: Organize files
    if msg_files:
        organize_files(msg_files)
    
    # Step 4: Verify
    verify_setup()
    
    # Step 5: Next steps
    show_next_steps()
    
    print("\n" + "="*70)
    print("✅ Setup Complete!")
    print("="*70 + "\n")


if __name__ == "__main__":
    main()